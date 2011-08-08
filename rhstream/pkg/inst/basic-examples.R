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

## lapply like job, first intro


small.ints = 1:10
lapply(small.ints, function(x) x^2)

small.ints = rhwrite(1:10)
revoMapReduce(input=small.ints, map = function(k,v) keyval(k^2))

rhread(revoMapReduce(input=small.ints, map = function(k,v) keyval(k^2)))



## classic wordcount 
##input can be any text file
## inspect output with rhread(output) -- this will produce an R list watch out with big datasets

rhwordcount = function (input, output, pattern = " ") {
  revoMapReduce(input = input ,
                output = output,
                textinputformat = rawtextinputformat,
                map = function(k,v) {
                  lapply(
                         strsplit(
                                  x = v,
                                  split = pattern)[[1]],
                         function(w) keyval(w,1))},
                reduce = function(k,vv) {
                  keyval(k, sum(unlist(vv)))},
                combine = T)}

##input can be any RevoStreaming file (our own format)
## pred can be function(x) x > 0
## it will be evaluated on the value only, not on the key
## test set: rhwrite (lapply (1:10, function(i) keyval(rnorm(2))), "/tmp/filtertest")
## run with mrfilter("/tmp/filtertest", "/tmp/filterout", function(x) x > 0)
## inspect results with rhread("/tmp/filterout")

filtermap= function(pred) function(k,v) {if (pred(v)) keyval(k,v) else NULL}

rhfilter = function (input, output, pred) {
  revoMapReduce(input = input,
           output = output,
           map = filtermap(pred))
}

## pipeline of two filters, sweet
# rhread(mrfilter(input = mrfilter(
#                  input = "/tmp/filtertest/",
#                  pred = function(x) x > 0),
#                pred = function(x) x < 0.5))
