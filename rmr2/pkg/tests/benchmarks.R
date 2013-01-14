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

library(rmr2)
#timings from macbook pro i7 2011, standalone CDH4, one core, SDD

report = list()
for (be in c("local", "hadoop")) {
  rmr.options(backend = be)
## @knitr input
  input.size = {  
    if(rmr.options('backend') == "local") 
      10^4   
    else 
      10^6} 
## @knitr end
  report[[be]] =
    rbind(
      report[[be]], 
      write = 
        system.time({
          out = 
## @knitr write
  input = to.dfs(1:input.size)
## @knitr end  
        }))
  
  report[[be]] =
    rbind(
      report[[be]],
      read = 
        system.time({
          out = 
## @knitr read
  from.dfs(input)
## @knitr end        
        }))
  stopifnot(
    all(
      1:input.size == sort(values(out))))
  
  report[[be]] =
    rbind(
      report[[be]],
      pass.through = system.time({
        out = 
## @knitr pass-through
  mapreduce(
    input, 
    map = function(k, v) keyval(k, v))
## @knitr end        
      }))
  stopifnot(
    all(
      1:input.size == 
        sort(values(from.dfs(out)))))  
  
## @knitr predicate            
  predicate = 
    function(., v) unlist(v)%%2 == 0
## @knitr end            
  report[[be]] =
    rbind(
      report[[be]],
      filter = system.time({
        out = 
## @knitr filter              
  mapreduce(
    input, 
    map = 
      function(k, v) {
        filter = predicate(k, v); 
        keyval(k[filter], v[filter])}
## @knitr end                               
          )}))
  stopifnot(
    all(
      2*(1:(input.size/2)) == 
        sort(values(from.dfs(out)))))
  
## @knitr select-input           
  input.select = 
    to.dfs(
      data.frame(
        a = rnorm(input.size),
        b = 1:input.size,
        c = sample(as.character(1:10),
                   input.size, 
                   replace=TRUE)))
## @knitr end             
  report[[be]] =
    rbind(
      report[[be]],
      select = system.time({
        out = 
## @knitr select                 
  mapreduce(input.select,
            map = function(., v) v$b)
## @knitr end                                   
      }))
  stopifnot(
    all(
      1:input.size == 
        sort(values(from.dfs(out)))))
  
## @knitr bigsum-input
  set.seed(0)
  big.sample = rnorm(input.size)
  input.bigsum = to.dfs(big.sample)
## @knitr end 
  report[[be]] =
    rbind(
      report[[be]],
      bigsum = system.time({
        out = 
## @knitr bigsum                
  mapreduce(
    input.bigsum, 
    map  = 
      function(., v) keyval(1, sum(v)), 
    reduce = 
      function(., v) keyval(1, sum(v)),
    combine = TRUE)
## @knitr end                                   
      }))
  stopifnot(
    isTRUE(
      all.equal(
        sum(values(from.dfs(out))), 
        sum(big.sample), 
        tolerance=.000001)))
## @knitr group-aggregate-input
  input.ga = 
    to.dfs(
      cbind(
        1:input.size,
        rnorm(input.size)))
## @knitr group-aggregate-functions
  group = function(x) x%%10
  aggregate = function(x) sum(x)
  rmr.options(keyval.length=10^4)
## @knitr end             
  report[[be]] =
    rbind(
      report[[be]],
      group.aggregate = system.time({
        out = 
## @knitr group-aggregate
  mapreduce(
    input.ga, 
      map = 
        function(k, v) 
          keyval(group(v[,1]), v[,2]),
      reduce = 
        function(k, vv) 
          keyval(k, aggregate(vv)),
      combine = TRUE)
## @knitr end                                   
      }))
}
print(report)
# 
# $hadoop
# user.self sys.self elapsed user.child sys.child
# write               1.155    0.043   3.688      2.465     0.315
# read                0.843    0.099   3.028      3.201     0.269
# pass.through        0.887    0.014   5.472      5.375     0.749
# filter              0.173    0.007   4.760      5.025     0.773
# select              0.199    0.008   6.828      6.633     1.100
# bigsum              0.728    0.024   6.683      6.683     0.970
# group.aggregate     0.726    0.029  12.860     12.260     1.595
