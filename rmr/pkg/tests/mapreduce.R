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
library(rmr)

for (be in c("local", "hadoop")) {
  rmr.options(backend = be)
  
  
  ## keyval compare
  kv.cmp = function(kv1, kv2) {
    kv1 = rmr:::split.keyval(kv1)
    kv2 = rmr:::split.keyval(kv2)
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
            generators = list(rmr:::tdgg.keyval()),
            sample.size = 10)
  
  ## csv
  unit.test(function(df) {
    isTRUE(
      all.equal(
        df, 
        values(
          from.dfs(
            to.dfs(df, 
                   format = "csv"), 
            format = make.input.format(
              format = "csv", 
              colClasses = lapply(df[1,], class)))), 
        tolerance = 1e-4, 
        check.attributes = FALSE))},
            generators = list(tdgg.data.frame()),
            sample.size = 10)
  
#   
#   for(fmt in c("json", "sequence.typedbytes")) {
#     unit.test(function(df,fmt) {
#       isTRUE(all.equal(df, values(from.dfs(to.dfs(df, format = fmt), format = fmt), tolerance = 1e-4, check.attributes = FALSE)))},
#               generators = list(tdgg.data.frame(), tdgg.constant(fmt)),
#               sample.size = 10)}
  
  ##mapreduce
 
  ##simplest mapreduce, all default
  unit.test(function(kv) {
    if(length(kv) == 0) TRUE
    else {
      kv1 = from.dfs(mapreduce(input = to.dfs(kv)))
      kv.cmp(kv, kv1)}},
            generators = list(rmr:::tdgg.keyval()),
            sample.size = 10)
  
  ##put in a reduce for good measure
  unit.test(function(kv) {
    if(length(kv) == 0) TRUE
    else {
      kv1 = from.dfs(mapreduce(input = to.dfs(kv),
                                reduce = to.reduce(identity)))
      kv.cmp(kv, kv1)}},
            generators = list(rmr:::tdgg.keyval()),
            sample.size = 10)
  
#   for(fmt in c("json", "sequence.typedbytes")) {
#     unit.test(
#       function(df,fmt) {
#         isTRUE(
#           all.equal(
#             df, 
#             values(
#               from.dfs(
#                 mapreduce(
#                   to.dfs(
#                     df, 
#                     format = fmt),
#                   reduce = to.reduce.all(identity),
#                   input.format = fmt,
#                   output.format = fmt),
#                 format = fmt)), 
#             tolerance = 1e-4, check.attributes = FALSE))},
#       generators = list(tdgg.data.frame(), tdgg.constant(fmt)),
#       sample.size = 10)}
  
  ## csv
  library(digest)
  data.frame.order = function(x) order(apply(x, 1, function(y) digest(as.character(y))))
  unit.test(function(df) {
    inpf = make.input.format(
      format = "csv", 
      colClasses = lapply(df[1,], class))
    df1 = 
      values(
        from.dfs(
          mapreduce(
            to.dfs(
              df, 
              format = "csv"),
            input.format = inpf,
            output.format = "csv"),
          format = inpf))
    isTRUE(
      all.equal(
        df[data.frame.order(df),], 
        df1[data.frame.order(df1),], 
        tolerance = 1e-4, 
        check.attributes = FALSE))},
            generators = list(tdgg.data.frame()),
            sample.size = 10)
  
  
}                                           
