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

# lapply like job, first intro

library(rmr)

for (be in c("local", "hadoop")) {
  rmr.options.set(backend = be)
## @knitr lapply  
  small.ints = 1:1000
  lapply(small.ints, function(x) x^2)
## @knitr end
## @knitr lapply-mapreduce
  small.ints = to.dfs(1:1000)
  mapreduce(input = small.ints, map = function(k,v) keyval(v, v^2))
## @knitr end
  from.dfs(mapreduce(input = small.ints, map = function(k,v) keyval(v, v^2)))
  
  # tapply like job
## @knitr tapply
  groups = rbinom(32, n = 50, prob = 0.4)
  tapply(groups, groups, length)
## @knitr end
## @knitr tapply-mapreduce
  groups = to.dfs(groups)
  from.dfs(mapreduce(input = groups, map = function(k,v) keyval(v, 1), reduce = function(k,vv) keyval(k, length(vv))))
## @knitr end 
  
  ## input can be any rmr-native format file
  ## pred can be function(x) x > 0
  ## it will be evaluated on the value only, not on the key
  
## @knitr  filter  
  filtermap = function(pred) function(k,v) {if (pred(v)) keyval(k,v) else NULL}
  
  mrfilter = function (input, output = NULL, pred) {
    mapreduce(input = input,
              output = output,
              map = filtermap(pred))
  }

  filtertest = to.dfs(lapply (1:10, function(i) keyval(NULL, rnorm(1))))
  from.dfs(mrfilter(input = filtertest, pred = function(x) x > 0))}
## @knitr end

## pipeline of two filters, sweet
# from.dfs(mrfilter(input = mrfilter(
#                  input = "/tmp/filtertest/",
#                  pred = function(x) x > 0),
#                pred = function(x) x < 0.5))
