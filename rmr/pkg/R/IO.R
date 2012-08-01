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
                lapply.nrecs(splits, function(x) fromJSON(x[2], asText = TRUE), nrecs = nrecs),
                vectorized = nrecs > 1)}}

json.output.format = function(k, v, con, vectorized) {
  ser = function(k, v) paste(gsub("\n", "", toJSON(k, .escapeEscapes=TRUE, collapse = "")),
                             gsub("\n", "", toJSON(v, .escapeEscapes=TRUE, collapse = "")),
                             sep = "\t")
  out = 
    if(vectorized)
      mapply(k,v, ser)
  else
    ser(k,v)
  writeLines(out, con = con, sep = "\n")}

text.input.format = function(con, nrecs) {
  lines = readLines(con, nrecs)
  if (length(lines) == 0) NULL
  else keyval(NULL, lines, vectorized = nrecs > 1)}

text.output.format = function(k, v, con, vectorized) {
  ser = function(k,v) paste(k, v, collapse = "", sep = "\t")
  out = if(vectorized)
    mapply(ser, k, v)
  else
    ser(k,v)
  writeLines(out, sep = "\n", con = con)}

csv.input.format = function(...) function(con, nrecs) {
  df = 
    tryCatch(
      read.table(file = con, nrows = nrecs, header = FALSE, ...),
      error = function(e) NULL)
  if(is.null(df) || dim(df)[[1]] == 0) NULL
  else keyval(NULL, df, vectorized = nrecs > 1)}

csv.output.format = function(...) function(k, v, con, vectorized) 
  # this is vectorized only, need to think what that means
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
          keyval(obj.buffer[[1]], obj.buffer[[2]], vectorized = FALSE)
        else keyval(obj.buffer[2*(1:actual.recs) - 1],
                  obj.buffer[2*(1:actual.recs)], 
                  vectorized = TRUE)}
    if(actual.recs > 0) obj.buffer <<- obj.buffer[-(1:(2*actual.recs))]
    retval}}
  
typed.bytes.output.format = function(k, v, con, vectorized){
  writeBin(
    typed.bytes.writer(
      if(vectorized){
        k = to.list(k)
        v = to.list(v)
        tmp = list()
        tmp[2*(1:length(k)) - 1] = k
        tmp[2*(1:length(k))] = v
        tmp}
      else {
        list(k,v)}),
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

native.output.format = function(k, v, con, vectorized){
  if(vectorized)
    typed.bytes.output.format(k, v, con, vectorized)
  else {
    native.writer(k, con)
    native.writer(v, con)}}