# Copyright 2011 Revolution Analytics
#    
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

lapply.nrecs = function(..., nrecs) {
  out = lapply(...)
  if(nrecs == 1) out[[1]] else out}

json.input.format = function(con, nrecs) {
  lines = readLines(con, nrecs)
  if (length(lines) == 0) NULL
  else {
    splits =  strsplit(lines, "\t")
    if(length(splits[[1]]) == 1)  keyval(NULL, lapply.nrecs(splits, function(x) fromJSON(x[1], asText = TRUE), nrecs = nrecs))
    else keyval(lapply.nrecs(splits, function(x) fromJSON(x[1], asText = TRUE), nrecs = nrecs), 
                lapply.nrecs(splits, function(x) fromJSON(x[2], asText = TRUE), nrecs = nrecs))}}

json.output.format = function(k, v, con) {
  ser = function(k, v) paste(gsub("\n", "", toJSON(k, .escapeEscapes=TRUE, collapse = "")),
                             gsub("\n", "", toJSON(v, .escapeEscapes=TRUE, collapse = "")),
                             sep = "\t")
  out = mapply(k,v, ser)
  writeLines(out, con = con, sep = "\n")}

text.input.format = function(con, nrecs) {
  lines = readLines(con, nrecs)
  if (length(lines) == 0) NULL
  else keyval(NULL, lines)}

text.output.format = function(k, v, con) {
  ser = function(k,v) paste(k, v, collapse = "", sep = "\t")
  out = mapply(ser, k, v)
  writeLines(out, sep = "\n", con = con)}

csv.input.format = function(...) function(con, nrecs) {
  df = 
    tryCatch(
      read.table(file = con, nrows = nrecs, header = FALSE, ...),
      error = function(e) NULL)
  if(is.null(df) || dim(df)[[1]] == 0) NULL
  else keyval(NULL, df)}

csv.output.format = function(...) function(k, v, con) 
  write.table(file = con, 
              x = if(is.null(k)) v else cbind(k,v), 
              ..., 
              row.names = FALSE, 
              col.names = FALSE)

typed.bytes.reader = function(data, nobjs) {
  if(is.null(data)) NULL
  else
    .Call("typed_bytes_reader", data, nobjs, PACKAGE = "rmr") 
}
typed.bytes.writer = function(objects) {
  .Call("typed_bytes_writer", objects, PACKAGE = "rmr")
}

typed.bytes.input.format = function() {
  obj.buffer = list()
  raw.buffer = raw()
  read.size = 1000
  function(con, nrecs) {
    nobjs = 2*nrecs
    while(length(obj.buffer) < nobjs) {
      raw.buffer <<- c(raw.buffer, readBin(con, raw(), read.size))
      if(length(raw.buffer) == 0) break;
      parsed = typed.bytes.reader(raw.buffer, as.integer(read.size/2))
      obj.buffer <<- c(obj.buffer, parsed$objects)
      if(parsed$length != 0) raw.buffer <<- raw.buffer[-(1:parsed$length)]
      read.size = as.integer(1.2 * read.size)}
    read.size = as.integer(read.size/1.2)
    actual.recs = min(nrecs, length(obj.buffer)/2)
    retval = if(length(obj.buffer) == 0) NULL 
      else { 
        if(nrecs == 1)
          keyval(obj.buffer[[1]], obj.buffer[[2]])
        else keyval(obj.buffer[2*(1:actual.recs) - 1],
                  obj.buffer[2*(1:actual.recs)])}
    if(actual.recs > 0) obj.buffer <<- obj.buffer[-(1:(2*actual.recs))]
    retval}}
  
typed.bytes.output.format = function(k, v, con){
  writeBin(
    typed.bytes.writer(
        k = to.list(k)
        v = to.list(v)
        tmp = list()
        tmp[2*(1:length(k)) - 1] = k
        tmp[2*(1:length(k))] = v
        tmp),
    con)}

native.input.format = typed.bytes.input.format

native.writer = function(value, con) {
  w = function(x, size = NA_integer_) writeBin(x, con, size = size, endian = "big")
  write.code = function(x) w(as.integer(x), size = 1)
  write.length = function(x) w(as.integer(x), size = 4)
  bytes = serialize(value, NULL)
  write.code(144) 
  write.length(length(bytes))
  w(bytes)
  TRUE}

native.output.format = function(k, v, con){
  # temporarily disabled    typed.bytes.output.format(k, v, con, vectorized)
  lapply(1:rmr.length(k),
         function(i) {
           native.writer(rmr.slice(k, i), con)
           native.writer(rmr.slice(v, i), con)}}

# I/O 

make.record.reader = function(mode = NULL, format = NULL, con = NULL, nrecs = 1) {
  default = make.input.format()
  if(is.null(mode)) mode = default$mode
  if(is.null(format)) format = default$format
  if(mode == "text") {
    if(is.null(con)) con = file("stdin", "r")} #not stdin() which is parsed by the interpreter
  else {
    if(is.null(con)) con = pipe("cat", "rb")}
  function() format(con, nrecs)}

make.record.writer = function(mode = NULL, format = NULL, con = NULL) {
  default = make.output.format()
  if(is.null(mode)) mode = default$mode
  if(is.null(format)) format = default$format
  if(mode == "text") {
    if(is.null(con)) con = stdout()}
  else {
    if(is.null(con)) con = pipe("cat", "wb")}
  function(k, v) format(k, v, con)}

IO.formats = c("text", "json", "csv", "native",
               "sequence.typedbytes")

make.input.format = function(format = native.input.format(), 
                             mode = c("binary", "text"),
                             streaming.format = NULL, ...) {
  mode = match.arg(mode)
  if(is.character(format)) {
    format = match.arg(format, IO.formats)
    switch(format, 
           text = {format = text.input.format 
                   mode = "text"}, 
           json = {format = json.input.format 
                   mode = "text"}, 
           csv = {format = csv.input.format(...) 
                  mode = "text"}, 
           native = {format = native.input.format() 
                     mode = "binary"}, 
           sequence.typedbytes = {format = typed.bytes.input.format() 
                                  mode = "binary"})}
  if(is.null(streaming.format) && mode == "binary") 
    streaming.format = "org.apache.hadoop.streaming.AutoInputFormat"
  list(mode = mode, format = format, streaming.format = streaming.format)}

make.output.format = function(format = native.output.format, 
                              mode = c("binary", "text"),
                              streaming.format = "org.apache.hadoop.mapred.SequenceFileOutputFormat", 
                              ...) {
  mode = match.arg(mode)
  if(is.character(format)) {
    format = match.arg(format, IO.formats)
    switch(format, 
           text = {format = text.output.format
                   mode = "text"
                   streaming.format = NULL},
           json = {format = json.output.format
                   mode = "text"
                   streaming.format = NULL}, 
           csv = {format = csv.output.format(...)
                  mode = "text"
                  streaming.format = NULL}, 
           native = {format = native.output.format 
                     mode = "binary"
                     streaming.format = "org.apache.hadoop.mapred.SequenceFileOutputFormat"}, 
           sequence.typedbytes = {format = typed.bytes.output.format 
                                  mode = "binary"
                                  streaming.format = "org.apache.hadoop.mapred.SequenceFileOutputFormat"})}
  mode = match.arg(mode)
  list(mode = mode, format = format, streaming.format = streaming.format)}

