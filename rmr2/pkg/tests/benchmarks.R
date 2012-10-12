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
#timings from macbook pro i7 2011, standalone CDH3, one core
  
for (be in c("local", "hadoop")) {
  rmr.options(backend = be)
## @knitr input
  input.size = {  
    if(rmr.options('backend') == "local") 
      10^4     
    else 
      10^6} 
## @knitr end
  system.time({out = 
## @knitr write
    input = to.dfs(1:input.size)
## @knitr end  
  })
  #   user  system elapsed 
  #   3.376   0.217   2.877 
  system.time({out = 
## @knitr read
    from.dfs(input)
## @knitr end        
  })
  #   user  system elapsed 
  #   9.466   0.266   8.403
  stopifnot(
    all(
      1:input.size == sort(values(out))))
  
  system.time({out = 
## @knitr pass-through
    mapreduce(
      input, 
      map = function(k, v) keyval(k, v))
## @knitr end        
              })
  #   user  system elapsed 
  #   10.160   0.764   9.443 
  stopifnot(
    all(
      1:input.size == 
        sort(values(from.dfs(out)))))  
  
## @knitr predicate            
  predicate = 
    function(., v) unlist(v)%%2 == 0
## @knitr end            
  system.time({out = 
## @knitr filter              
    mapreduce(
      input, 
      map = 
        function(k, v) {
          filter = predicate(k, v); 
          keyval(k[filter], v[filter])})
## @knitr end                               
  })
  #   user  system elapsed 
  #   8.671   0.868   8.162 
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
  system.time({out = 
## @knitr select                 
    mapreduce(input.select,
              map = function(., v) v$b)
## @knitr end                                   
              })
#   user  system elapsed 
#   11.737   0.712  10.745
  stopifnot(
    all(
      1:input.size == 
        sort(values(from.dfs(out)))))
  
## @knitr bigsum-input
  set.seed(0)
  big.sample = rnorm(input.size)
  input.bigsum = to.dfs(big.sample)
## @knitr end 
  system.time({out = 
## @knitr bigsum                
    mapreduce(
      input.bigsum, 
      map  = 
        function(., v) keyval(1, sum(v)), 
      reduce = 
        function(., v) keyval(1, sum(v)),
      combine = TRUE)
## @knitr end                                   
  })
#   user  system elapsed 
#   10.542   0.863   9.672 
  stopifnot(
    isTRUE(
      all.equal(
        sum(values(from.dfs(out))), 
        sum(big.sample), 
        tolerance=.000001)))
## @knitr group-aggregate-input
  input.ga = 
    to.dfs(
      keyval(
        1:input.size,
        rnorm(input.size)))
## @knitr group-aggregate-functions
  group = function(k, v) k%%100
  aggregate = function(x) sum(x)
## @knitr end             
  system.time({out = 
## @knitr group-aggregate
    mapreduce(
      input.ga, 
      map = 
        function(k, v) 
          keyval(group(k, v), v),
      reduce = 
        function(k, vv) 
          keyval(k, aggregate(vv)),
      combine = TRUE)
## @knitr end                                   
              })
#   user  system elapsed 
#   156.644   2.301 150.737 
  }
  
