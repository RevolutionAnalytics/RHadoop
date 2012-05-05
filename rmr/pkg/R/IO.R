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

native.text.input.format = function(con, nrecs) {
  lines  = readLines(con, nrecs)
  if (length(lines) == 0) NULL
  else {
    splits = strsplit(lines, "\t")
    deser.one = function(x) unserialize(charToRaw(gsub("\\\\n", "\n", x)))
    keyval(lapply.nrecs(splits, function(x) de(x[1]), nrecs = nrecs),
           lapply.nrecs(splits, function(x) de(x[2]), nrecs = nrecs),
           vectorized = nrecs > 1)}}

native.text.output.format = function(k, v, con, vectorized) {
  ser = function(x) gsub("\n", "\\\\n", rawToChar(serialize(x, ascii=T, conn = NULL)))
  ser.pair = function(k,v) paste(ser(k), ser(v), sep = "\t")
  out = 
    if(vectorized)
      mapply(ser.pair, k, v)
  else ser.pair(k,v)
  writeLines(out, con = con, sep = "\n")}

json.input.format = function(con, nrecs) {
  lines = readLines(con, nrecs)
  if (length(lines) == 0) NULL
  else {
    splits =  strsplit(lines, "\t")
    if(length(splits[[1]]) == 1)  keyval(NULL, lapply.nrecs(splits, function(x) fromJSON(x[1], asText = TRUE), nrecs = nrecs))
    else keyval(lapply.nrecs(splits, function(x) fromJSON(x[1], asText = TRUE), nrecs = nrecs), 
                lapply.nrecs(splits, function(x) fromJSON(x[2], asText = TRUE), nrecs = nrecs),
                vectorized = nrecs == 1)}}

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
  else keyval(NULL, lines, vectorized = nrecs == 1)}

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

typed.bytes.reader = 
  function(buf.size = 1000) {
    raw.buffer = raw()
    buffered.readBin = function(con, what, n = 1, size = NULL, ...) {
      if(what == "raw") size = 1
      raw.size = n * size
      if(raw.size <= length(raw.buffer)){
        retval = readBin(raw.buffer[1:(size*n)], what, n, size, ...)
        raw.buffer <<- raw.buffer[-(1:(size*n))]
        retval
      }
      else {
        raw.buffer <<- c(raw.buffer, readBin(con, "raw", buf.size, ...))
        if(length(raw.buffer) > 0) buffered.readBin(con, what, n, size, ...)
        else NULL
      }
    }
    reader = function (con, type.code = NULL) {
      r = function(...) {
        buffered.readBin(con, endian = "big", signed = TRUE, ...)}
      read.code = function() (256 + r(what = "integer", n = 1, size = 1)) %% 256
      read.length = function() r(what = "integer", n= 1, size = 4)
      tbr = function() reader(con)[[1]]
      two55.terminated.list = function() {
        ll = list()
        code = read.code()
        while(length(code) > 0 && code != 255) {
          ll = append(ll,  reader(con, code))
          code = read.code()}
        ll}#quadratic, fix later
      
      ##      if(is.raw(con)) 
      ##        con = rawConnection(con, open = "r") 
      if (is.null(type.code)) type.code = read.code()
      if(length(type.code) > 0) {
        list(switch(as.character(as.integer(type.code)), 
                    "0" = r("raw", n = read.length()), 
                    "1" = r("raw"),                    
                    "2" = r("logical", size = 1),      
                    "3" = r("integer", size = 4),      
                    "4" = r("integer", size = 8),      
                    "5" = r("numeric", size = 4),      
                    "6" = r("numeric", size = 8),      
                    "7" = rawToChar(r("raw", n = read.length())), 
                    "8" = replicate(read.length(), tbr(), simplify=FALSE), 
                    "9" = two55.terminated.list(), 
                    "10" = replicate(read.length(), keyval(tbr(), tbr()), simplify = FALSE),
                    "11" = r("integer", size = 2), #this and the next implemented only in hive, silly
                    "12" = NULL,
                    "144" = unserialize(r("raw", n = read.length()))))} 
      else NULL}
    reader}

typed.bytes.writer = function(value, con, native  = FALSE) {
  w = function(x, size = NA_integer_) writeBin(x, con, size = size, endian = "big")
  write.code = function(x) w(as.integer(x), size = 1)
  write.length = function(x) w(as.integer(x), size = 4)
  tbw = function(x) typed.bytes.writer(x, con)
  if(native) {
    bytes = serialize(value, NULL)
    write.code(144); write.length(length(bytes)); w(bytes)
  }
  else{
    if(is.list(value) && all(sapply(value, is.keyval))) {
      write.code(10)
      write.length(length(value))
      lapply(value, function(kv) {lapply(kv, tbw)})}
    else {
      if(length(value) == 1) {
        switch(class(value), 
               raw = {write.code(1); w(value)}, 
               logical = {write.code(2); w(value, size = 1)}, 
               integer = {write.code(3); w(value)}, 
               #doesn't happen in R integer = {write.code(4); w(value)}, 
               #doesn't happen in R numeric = {write.code(5); w(value)}, 
               numeric = {write.code(6); w(value)}, 
               character = {write.code(7); write.length(nchar(value)); writeChar(value, con, eos = NULL)}, 
               factor = {value = as.character(value)
                         write.code(7); write.length(nchar(value)); writeChar(value, con, eos = NULL)},
               list = {write.code(8); write.length(1); tbw(value[[1]])},
               stop("not implemented yet"))}
      else {
        switch(class(value), 
               raw = {write.code(0); write.length(length(value)); w(value)}, 
               #NULL = {write.code(12); write.length(0)}, #this spec was added by the hive folks, but not in streaming HIVE-1029
               {write.code(8); write.length(length(value)); lapply(value, tbw)})}}}
  TRUE}

typed.bytes.input.format = function() {
  tbr = typed.bytes.reader()
  function(con, nrecs) {
    out = replicate(2*nrecs, tbr(con), simplify = FALSE)
    out = out[!sapply(out, is.null)]
    out = lapply(out, function(o)o[[1]])
    if(length(out) == 0) NULL
    else {
      if(nrecs == 1)
        keyval(out[[1]][[1]],out[[2]][[1]], vectorized = FALSE)
      else
        keyval(out[2*(1:(length(out)/2)) - 1], out[2*(1:(length(out)/2))], vectorized = TRUE)}}}

typed.bytes.output.format = function(k, v, con, vectorized) {
  ser = function(k,v) {
    typed.bytes.writer(k, con)
    typed.bytes.writer(v, con)
  }
  if(vectorized)
    mapply(ser, k, v)
  else
    ser(k,v)}

typed.bytes.Cpp.reader = function(data, nobjs) {
    .Call("typed_bytes_reader", data, nobjs, PACKAGE = "rmr") 
}
typed.bytes.Cpp.writer = function(objects) {
  .Call("typed_bytes_writer", objects, PACKAGE = "rmr")
}

typed.bytes.Cpp.input.format = function() {
  obj.buffer = list()
  raw.buffer = raw()
  read.size = 1000
  function(con, nrecs) {
    nobjs = 2*nrecs
    while(length(obj.buffer) < nobjs) {
      raw.buffer <<- c(raw.buffer, readBin(con, raw(), read.size))
      if(length(raw.buffer) == 0) break;
      parsed = typed.bytes.Cpp.reader(raw.buffer, 1000)
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
  
typed.bytes.Cpp.output.format = function(k, v, con, vectorized){
  writeBin(
    typed.bytes.Cpp.writer(
      if(vectorized){
        tmp = list()
        tmp[2*(1:length(k)) - 1] = k
        tmp[2*(1:length(k))] = v
        tmp}
      else {
        list(k,v)}),
    con)}

native.input.format = typed.bytes.Cpp.input.format

native.output.format = function(k, v, con, vectorized){
  ser.native = function(k,v) {
    typed.bytes.writer(k, con, TRUE)
    typed.bytes.writer(v, con, TRUE)
  }
  if(vectorized)
    typed.bytes.Cpp.output.format(k, v, con, vectorized)
  else
    ser.native(k,v)}
