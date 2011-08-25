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


## classic wordcount 
## input can be any text file
## inspect output with from.dfs(output) -- this will produce an R list watch out with big datasets

library(rmr)

wordcount = function (input, output = NULL, pattern = " ") {
  mapreduce(input = input ,
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

rmr:::dfs.put("/etc/passwd", "/tmp/wordcount-test")
from.dfs(wordcount("/tmp/wordcount-test", pattern = " +"))
