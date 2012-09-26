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

library(quickcheck)
library(rmr2)

for (be in c("local", "hadoop")) {
  rmr.options(backend = be)
  
  
  ## keyval compare
  kv.cmp = function(kv1, kv2) {
    kv1 = rmr2:::split.keyval(kv1)
    kv2 = rmr2:::split.keyval(kv2)
    o1 = order(unlist(keys(kv1)))  
    o2 = order(unlist(keys(kv2)))
    isTRUE(all.equal(keys(kv1)[o1], keys(kv2)[o2], tolerance=1e-4, check.attributes=FALSE)) &&
      isTRUE(all.equal(values(kv1)[o1], values(kv2)[o2], tolerance=1e-4, check.attributes=FALSE)) }
  
  ##from.dfs to.dfs
  ##native
  unit.test(
    function(kv) {
      kv.cmp(kv, 
             from.dfs(to.dfs(kv)))},
            generators = list(rmr2:::tdgg.keyval()),
            sample.size = 10)
  
  ## csv
  unit.test(
    function(df) {
      isTRUE(
        all.equal(
          df, 
          values(
            from.dfs(
              to.dfs(
                df, 
                format = "csv"), 
              format = make.input.format(
                format = "csv"))), 
          tolerance = 1e-4, 
          check.attributes = FALSE))},
    generators = list(tdgg.data.frame()),
    sample.size = 10)
  
  #json
  fmt = "json"
  unit.test(
    function(df) {
      isTRUE(
        all.equal(
          df, 
          values(
            from.dfs(
              to.dfs(
                df, 
                format = fmt), 
              format = make.input.format("json", key.class = "list", value.class = "data.frame"))), 
          tolerance = 1e-4, 
          check.attributes = FALSE))},
    generators = list(tdgg.data.frame()),
    sample.size = 10)
  
  #sequence.typedbytes
  seq.tb.data.loss = 
    function(l)
      rapply(
        l,
        function(x) if(class(x) == "raw") x else as.list(x),
        how = "replace")    
    
  fmt = "sequence.typedbytes"
  unit.test(
    function(l) {
      isTRUE(
        all.equal(
          seq.tb.data.loss(l),
          values(
            from.dfs(
              to.dfs(
                keyval(1,l), 
                format = fmt), 
              format = fmt)), 
          tolerance = 1e-4, 
          check.attributes = FALSE))},
    generators = list(tdgg.list()),
    precondition = function(l) length(l) > 0,
    sample.size = 10)
  
  ##mapreduce
 
  ##simplest mapreduce, all default
  unit.test(function(kv) {
      if(rmr2:::length.keyval(kv) == 0) TRUE
    else {
      kv1 = from.dfs(mapreduce(input = to.dfs(kv)))
      kv.cmp(kv, kv1)}},
            generators = list(rmr2:::tdgg.keyval()),
            sample.size = 10)
  
  ##put in a reduce for good measure
  unit.test(function(kv) {
    if(length(kv) == 0) TRUE
    else {
      kv1 = from.dfs(mapreduce(input = to.dfs(kv),
                                reduce = to.reduce(identity)))
      kv.cmp(kv, kv1)}},
            generators = list(rmr2:::tdgg.keyval()),
            sample.size = 10)
  
  ## csv
  unit.test(
    function(df) {
      df1 = 
        values(
          from.dfs(
            mapreduce(
              to.dfs(
                df, 
                format = "csv"),
              input.format = "csv",
              output.format = "csv"),
            format = "csv"))
      isTRUE(
        all.equal(
          df[order(df[,1]),], 
          df1[order(df1[,1]),], 
          tolerance = 1e-4, 
          check.attributes = FALSE))},
    generators = list(tdgg.data.frame()),
    sample.size = 10)
  
  #json
  # a more general test would be better for json but the subtleties of mapping R to to JSON are many
  fmt = "json"
  unit.test(
    function(df) {
      df1 = 
        values(
          from.dfs(
            mapreduce(
              to.dfs(
                df, 
                format = fmt),
              input.format = make.input.format("json", key.class = "list", value.class = "data.frame"),
              output.format = fmt),
            format = make.input.format("json", key.class = "list", value.class = "data.frame")))
      isTRUE(
        all.equal(
          df[order(df[,1]),], 
          df1[order(df1[,1]),], 
          tolerance = 1e-4, 
          check.attributes = FALSE))},
    generators = list(tdgg.data.frame()),
    sample.size = 10)
  
  #sequence.typedbytes
  fmt = "sequence.typedbytes"
  unit.test(
    function(l) {
      l = seq.tb.data.loss(l)
      isTRUE(
        all.equal(
          l,     
          values(
            from.dfs(
              mapreduce(
                to.dfs(
                  keyval(1,l), 
                  format = fmt), 
                input.format = fmt,
                output.format = fmt),
              format = fmt)), 
          tolerance = 1e-4, 
          check.attributes = FALSE))},
    generators = list(tdgg.list()),
    precondition = function(l) length(l) > 0,
    sample.size = 10)
}