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

library(rmr2)

## @knitr wordcount-signature
wordcount = 
  function (input, output = NULL, pattern = " ") {
## @knitr wordcout-map
    wc.map = 
      function(k,v) {
        keyval(
          unlist(
            strsplit(
              x = v,
              split = pattern)),
          1)}
## @knitr wordcount-reduce
    wc.reduce =
      function(k,vv) {
        keyval(k, sum(vv))}
## @knitr wordcount-mapreduce
    mapreduce(input = input ,
              output = output,
              input.format = "text",
              map = wc.map,
              reduce = wc.reduce,
              combine = T)}
## @knitr end

rmr2:::hdfs.put("/etc/passwd", "/tmp/wordcount-test")
rmr.options(backend = "local")
out.local = from.dfs(wordcount("/tmp/wordcount-test", pattern = " +"))
rmr.options(backend = "hadoop")
out.hadoop = from.dfs(wordcount("/tmp/wordcount-test", pattern = " +"))
stopifnot(rmr2:::cmp(out.hadoop, out.local))
