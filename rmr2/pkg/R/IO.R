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

json.input.format = function(con, keyval.length) {
  warning("format not updated to new API")
  lines = readLines(con, keyval.length)
  if (length(lines) == 0) NULL
  else {
    splits =  strsplit(lines, "\t")
    keyval(lapply(splits, function(x) fromJSON(x[1], asText = TRUE)), 
           lapply(splits, function(x) fromJSON(x[2], asText = TRUE)))}}

json.output.format = function(kv, con) {
  warning("format not updated to new API")
  ser = function(k, v) paste(gsub("\n", "", toJSON(k, .escapeEscapes=TRUE, collapse = "")),
                             gsub("\n", "", toJSON(v, .escapeEscapes=TRUE, collapse = "")),
                             sep = "\t")
  out = apply.keyval(ser, kv)
  writeLines(out, con = con, sep = "\n")}

text.input.format = function(con, keyval.length) {
  lines = readLines(con, keyval.length)
  if (length(lines) == 0) NULL
  else keyval(NULL, lines)}

text.output.format = function(kv, con) {
  ser = function(k, v) paste(k, v, collapse = "", sep = "\t")
  out = apply.keyval(ser, kv, length.keyval(kv))
  writeLines(out, sep = "\n", con = con)}

make.csv.input.format = function(...) function(con, keyval.length) {
  df = 
    tryCatch(
      read.table(file = con, nrows = keyval.length, header = FALSE, ...),
      error = function(e) NULL)
  if(is.null(df) || dim(df)[[1]] == 0) NULL
  else keyval(NULL, df)}

make.csv.output.format = function(...) function(kv, con) {
  kv = recycle.keyval(kv)
  k = keys(kv)
  v = values(kv)
  write.table(file = con, 
              x = if(is.null(k)) v else cbind(k,v), 
              ..., 
              row.names = FALSE, 
              col.names = FALSE)}

typed.bytes.reader = function(data, nobjs) {
  if(is.null(data)) NULL
  else
    .Call("typed_bytes_reader", data, nobjs, PACKAGE = "rmr2")}

typed.bytes.writer = function(objects) {
  .Call("typed_bytes_writer", objects, PACKAGE = "rmr2")}

make.typed.bytes.input.format = function() {
  obj.buffer = list()
  raw.buffer = raw()
  read.size = 1000
  function(con, keyval.length) {
    while(length(obj.buffer) < 2 || 
      sum(sapply(even(obj.buffer), rmr.length)) < keyval.length) {
      raw.buffer <<- c(raw.buffer, readBin(con, raw(), read.size))
      if(length(raw.buffer) == 0) break;
      parsed = typed.bytes.reader(raw.buffer, as.integer(read.size/2))
      obj.buffer <<- c(obj.buffer, parsed$objects)
      if(parsed$length != 0) raw.buffer <<- raw.buffer[-(1:parsed$length)]
      read.size = as.integer(1.2 * read.size)}
    read.size = as.integer(read.size/1.2)
    straddler = list()
    retval = 
      if(length(obj.buffer) == 0) NULL 
      else { 
        if(length(obj.buffer)%%2 ==1) {
           straddler = obj.buffer[length(obj.buffer)]
           obj.buffer <<- obj.buffer[-length(obj.buffer)]}
        c.keyval(mapply(keyval, odd(obj.buffer), even(obj.buffer), SIMPLIFY = FALSE))}
    obj.buffer <<- straddler
    retval}}
  
typed.bytes.output.format = function(kv, con){
  warning("format not updated to new API")
  writeBin(
    typed.bytes.writer({
      k = to.list(k)
      v = to.list(v)
      interleave(kv)}),
    con)}

make.native.input.format = make.typed.bytes.input.format

native.writer =  function(objs, con) {
  w = function(x, size = NA_integer_) writeBin(x, con, size = size, endian = "big")
  write.code = function(x) w(as.integer(x), size = 1)
  write.length = function(x) w(as.integer(x), size = 4)
  lapply(objs,
         function(x) {
           bytes = serialize(x, NULL)
           write.code(144) 
           write.length(length(bytes))
           w(bytes)})
  TRUE}

make.native.output.format = 
  function(keyval.length)
    function(kv, con){
      # temporarily disabled    typed.bytes.output.format(kv, con)
      kvs = split.keyval(kv, keyval.length)
      native.writer(interleave(keys(kvs), values(kvs)), con)}

# I/O 

make.keyval.reader = function(mode, format, keyval.length, con = NULL) {
  if(mode == "text") {
    if(is.null(con)) con = file("stdin", "r")} #not stdin() which is parsed by the interpreter
  else {
    if(is.null(con)) con = pipe("cat", "rb")}
  function() 
    format(con, keyval.length)}

make.keyval.writer = function(mode, format, con = NULL) {
  if(mode == "text") {
    if(is.null(con)) con = stdout()}
  else {
    if(is.null(con)) con = pipe("cat", "wb")}
  function(kv) format(kv, con)}

IO.formats = c("text", "json", "csv", "native",
               "sequence.typedbytes")

make.input.format = function(format = make.native.input.format(), 
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
           csv = {format = make.csv.input.format(...) 
                  mode = "text"}, 
           native = {format = make.native.input.format() 
                     mode = "binary"}, 
           sequence.typedbytes = {format = make.typed.bytes.input.format() 
                                  mode = "binary"})}
  if(is.null(streaming.format) && mode == "binary") 
    streaming.format = "org.apache.hadoop.streaming.AutoInputFormat"
  list(mode = mode, format = format, streaming.format = streaming.format)}

make.output.format = function(format = make.native.output.format(),#rmr.options('keyval.length')), 
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
           csv = {format = make.csv.output.format(...)
                  mode = "text"
                  streaming.format = NULL}, 
           native = {format = make.native.output.format(keyval.length = rmr.options('keyval.length'))
                     mode = "binary"
                     streaming.format = "org.apache.hadoop.mapred.SequenceFileOutputFormat"}, 
           sequence.typedbytes = {format = typed.bytes.output.format 
                                  mode = "binary"
                                  streaming.format = "org.apache.hadoop.mapred.SequenceFileOutputFormat"})}
  mode = match.arg(mode)
  list(mode = mode, format = format, streaming.format = streaming.format)}

