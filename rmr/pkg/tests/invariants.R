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


source("quickcheck.R")

##app-specific generators
tdgg.keyval = function(keytdg = tdgg.double(), valtdg = tdgg.any()) function() keyval(keytdg(), valtdg())
tdgg.keyvalsimple = function() function() keyval(runif(1), runif(1)) #we can do better than this
tdgg.keyval.list = function(keytdg = tdgg.double(), valtdg = tdgg.list(), lambda = 100) tdgg.list(tdg = tdgg.keyval(keytdg, valtdg), lambda = lambda)

## to test output-related functions
catch.out = function(...) capture.output(invisible(...))

## actual tests

library(rmr)

for (be in c("local", "hadoop")) {
  rmr.options.set(backend = be)
  
  ## test for default writer TBA
  ##counter and status -- unused for now
  
  ## test for typed bytes read/write
  ## replace with unit.test when possible
  
  lapply(
    list(c(as.raw(66), as.raw(67)), #0
         as.raw(2), #1
         FALSE, #2
         10L, #4
         3.124, #6
         "foobar", #7 
         list(1,"ab", 1L), #8 
         list(keyval(1,"1"), keyval(2, "2"))), #10
    function(x) {
      fname = "/tmp/tbtest"
      con = file(fname, 'wb')
      rmr:::typed.bytes.writer(value=x, con=con)
      close(con)
      con = file(fname, 'rb')
      y = rmr:::typed.bytes.reader()(con=con)[[1]]
      close(con)
      stopifnot( all.equal(x,y))})
    
  ##keys and values
  unit.test(function(kvl) isTRUE(all.equal(kvl, 
                               apply(cbind(keys(kvl), 
                                           values(kvl)),1,function(x) keyval(x[[1]], x[[2]])))), 
           generators = list(tdgg.keyval.list()))
  ##keyval
  unit.test(function(kv) {
    isTRUE(all.equal(kv,keyval(kv$key,kv$val))) &&      
    attr(kv, "rmr.keyval")},
    generators = list(tdgg.keyval()))
  
  ##from.dfs to.dfs
  ##native
  unit.test(function(kvl) {
    isTRUE(
      all.equal(kvl, from.dfs(to.dfs(kvl))))},
    generators = list(tdgg.keyval.list()),
    sample.size = 10)
  
  ## csv
  unit.test(function(df) {
    isTRUE(
      all.equal(
        df, 
        from.dfs(
          to.dfs(df, 
                 format = "csv"), 
          format = make.input.format(
            format = "csv", 
            colClasses = lapply(df[1,], class)), 
          structured = TRUE), 
        tolerance = 1e-4, 
        check.attributes = FALSE))},
            generators = list(tdgg.data.frame()),
            sample.size = 10)
  
  
  for(fmt in c("json", "sequence.typedbytes")) {
    unit.test(function(df,fmt) {
      isTRUE(all.equal(df, from.dfs(to.dfs(df, format = fmt), format = fmt, to.data.frame = TRUE), tolerance = 1e-4, check.attributes = FALSE))},
              generators = list(tdgg.data.frame(), tdgg.constant(fmt)),
              sample.size = 10)}
  
  ##mapreduce
  
  ##unordered compare
  kvl.cmp = function(l1, l2) {
    l1 = l1[order(unlist(keys(l1)))]  
    l2 = l2[order(unlist(keys(l2)))]
    isTRUE(all.equal(l1, l2, tolerance=1e-4, check.attributes=FALSE))}
  
  ##simplest mapreduce, all default
  unit.test(function(kvl) {
    if(length(kvl) == 0) TRUE
    else {
      kvl1 = from.dfs(mapreduce(input = to.dfs(kvl)))
      kvl.cmp(kvl, kvl1)}},
           generators = list(tdgg.keyval.list(lambda = 10)),
           sample.size = 10)
  
  ##put in a reduce for good measure
  unit.test(function(kvl) {
    if(length(kvl) == 0) TRUE
    else {
      kvl1 = from.dfs(mapreduce(input = to.dfs(kvl),
                                reduce = to.reduce.all(identity)))
      kvl.cmp(kvl, kvl1)}},
            generators = list(tdgg.keyval.list(lambda = 10)),
            sample.size = 10)
  
  for(fmt in c("json", "sequence.typedbytes")) {
    unit.test(function(df,fmt) {
      isTRUE(all.equal(df, from.dfs(mapreduce(to.dfs(df, format = fmt),
                                              reduce = to.reduce.all(identity),
                                              input.format = fmt,
                                              output.format = fmt),
                                    format = fmt, 
                                    to.data.frame = TRUE), 
                       tolerance = 1e-4, check.attributes = FALSE))},
              generators = list(tdgg.data.frame(), tdgg.constant(fmt)),
              sample.size = 10)}
  
  ## csv
  library(digest)
  data.frame.order = function(x) order(apply(x, 1, function(y) digest(as.character(y))))
  unit.test(function(df) {
      inpf = make.input.format(
        format = "csv", 
        colClasses = lapply(df[1,], class))
      df1 = from.dfs(
        mapreduce(
          to.dfs(df, 
                 format = "csv"),
          input.format = inpf,
          output.format = "csv"),
        format = inpf, 
        to.data.frame = TRUE)
    isTRUE(
      all.equal(
        df[data.frame.order(df),], 
        df1[data.frame.order(df1),], 
        tolerance = 1e-4, 
        check.attributes = FALSE))},
            generators = list(tdgg.data.frame()),
            sample.size = 10)
  
  
  }                                           
