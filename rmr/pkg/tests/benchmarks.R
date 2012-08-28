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

library(rmr)
#timings from macbook pro i7 2011, standalone CDH3, one core
  
rmr.digest = function(input, output = NULL)
  mapreduce(input, output, map = function(k,v) {attr(v, "rmr.input") = NULL
                                                keyval(digest(keyval(k,v)), 1)})

test = function (out.1, out.2) {
  stopifnot(
    sort(unlist(keys(from.dfs(rmr.digest(out.1), vectorized = TRUE)))) ==
    sort(unlist(keys(from.dfs(rmr.digest(out.2), vectorized = TRUE)))))}

for (be in c("local", "hadoop")) {
  rmr.options.set(backend = be)
## @knitr input
  input.size = if(rmr.options.get('backend') == "local") 10^4 else 10^6
  input = to.dfs(1:input.size)
## @knitr  
  system.time({out = 
## @knitr read-write
    from.dfs(input)
## @knitr end        
  })
  stopifnot(all(1:input.size == sort(values(out))))
  #   user  system elapsed 
  #   9.633   3.727  12.671 
  
  system.time({out = 
## @knitr pass-through
    mapreduce(input, map = function(k,v) keyval(k,v))
## @knitr end        
              })
  stopifnot(all(1:input.size == sort(values(from.dfs(out)))))
  # user  system elapsed 
  # 46.370   1.830  42.669 
  
  
## @knitr predicate            
  predicate = function(k,v) unlist(v)%%2 == 0
## @knitr end            
  system.time({out = 
## @knitr filter              
    mapreduce(input, 
              map = function(k,v) {filter = predicate(k,v); 
                                   keyval(k[filter], v[filter])})
## @knitr end                               
                })
  stopifnot(all(2*(1:(input.size/2)) == sort(values(from.dfs(out)))))
  # user  system elapsed 
  # 44.716   1.707  40.361 
  
## @knitr select-input           
  input.select = to.dfs(data.frame(a=rnorm(input.size), b=1:input.size, c = as.character(1:input.size)))
## @knitr             
  system.time({out = 
## @knitr select                 
    mapreduce(input.select,
              map = function(k,v) v$b)
## @knitr                                   
              })
  stopifnot(all(1:input.size == sort(values(from.dfs(out)))))
  
## @knitr bigsum-input
  set.seed(0)
  input.bigsum = to.dfs(rnorm(input.size))
## @knitr 
  system.time({out = 
## @knitr bigsum                
    mapreduce(input.bigsum, 
              map  = function(k,v) sum(v), 
              reduce = function(k, vv) sum(vv),
              combine = TRUE)
## @knitr                                   
  })
  stopifnot(isTRUE(all.equal(sum(values(from.dfs(out))), 104.4752, tolerance=.000001)))
  ## @knitr group-aggregate-input
  input.ga = to.dfs(keyval(1:input.size, rnorm(input.size)))
  ## @knitr group-aggregate-functions
  group = function(k,v) k%%100
  aggregate = function(x) sum(x)
## @knitr             
  system.time({out = 
## @knitr group-aggregate
    mapreduce(input.ga, 
              map = function(k,v) keyval(group(k,v), v),
              reduce = function(k, vv) list(k, aggregate(vv)),
              combine = TRUE)
## @knitr                                   
              })
  
