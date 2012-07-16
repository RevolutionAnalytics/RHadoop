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
library(digest)
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
  data = keyval(rep(list(1), input.size),as.list(1:input.size), vectorized = TRUE)
  input = to.dfs(data)
## @knitr  
  system.time({out = 
## @knitr read-write
    from.dfs(input)
## @knitr         
  })
  stopifnot(rmr:::cmp(data, out))
  #   user  system elapsed 
  #   61.570   4.515  66.154 
## @knitr  
  system.time({out = 
## @knitr read-write-vec
    from.dfs(input, vectorized = TRUE)
## @knitr         
  })
  stopifnot(rmr:::cmp(data, out))
  #   user  system elapsed 
  #   9.633   3.727  12.671 
  
  system.time({out = 
## @knitr pass-through
    mapreduce(input, map = function(k,v) keyval(k,v))
## @knitr         
              })
  # user  system elapsed 
  # 179.345   4.671 179.111
  system.time({out.vec = 
## @knitr pass-through-vec         
    mapreduce(input,
              map = function(k,v) keyval(k,v, vectorized = TRUE), 
              vectorized = list(map = TRUE))
## @knitr                                   
              })
  # user  system elapsed 
  # 46.370   1.830  42.669 
  test(out, out.vec)  
   
## @knitr predicate            
  predicate = function(k,v) unlist(v)%%2 == 0
## @knitr             
  system.time({out = 
## @knitr filter                
    mapreduce(input,
              map = function(k,v) if(predicate(k,v)) keyval(k,v))
## @knitr                                   
               })
  # user  system elapsed 
  # 124.485   3.472 120.507 
  system.time({out.vec = 
## @knitr filter-vec              
    mapreduce(input, 
              map = function(k,v) {filter = predicate(k,v); 
                                   keyval(k[filter], v[filter], vectorized = TRUE)},
              vectorized = list(map = TRUE))
## @knitr                                   
                })
  # user  system elapsed 
  # 44.716   1.707  40.361 
  test(out, out.vec)  
  #structured says to convert list to data frame. Fails if not possible. If TRUE,
  #it means both map and reduce, or it is a named vector or list(map = TRUE, reduce = FALSE)
  #default both FALSE
  system.time({out.struct = 
## @knitr filter-vec-struct
    mapreduce(input, 
              map = function(k,v) {filter = predicate(k,v); 
                                   keyval(k[filter,1], v[filter,1], vectorized = TRUE)},
              vectorized = list(map = TRUE),
              structured = list(map = TRUE))
## @knitr                                   
              })
  test(out, out.struct)              
  
## @knitr select-input           
  input.select = to.dfs(keyval(1:input.size, 
                                    replicate(input.size, list(a=1,b=2,c=3), 
                                              simplify=FALSE), vectorized=TRUE))
## @knitr select-fun
  select = function(v) v[[2]]
## @knitr select-fun-vec
  select.vec = function(v) do.call(rbind,v)[,2] #names not preserved with current impl. of typedbytes
## @knitr select-fun-vec-struct
  field = 2
## @knitr             
  system.time({out = 
## @knitr select                 
    mapreduce(input.select,
              map = function(k,v) keyval(k, select(v)))
## @knitr                                   
              })
  # user  system elapsed 
  # 175.964   3.874 169.601 
  system.time({out.vec = 
## @knitr select-vec
    mapreduce(input.select,
              map = function(k,v) keyval(k, select.vec(v), vectorized = TRUE),
              vectorized = list(map = TRUE))
## @knitr                                   
              })
  # user  system elapsed 
  # 38.363   1.790  32.683
  test(out, out.vec)  
  system.time({out.struct = 
## @knitr select-vec-struct
    mapreduce(input = input.select,
              map = function(k,v) keyval(k[,1], v[,field], vectorized = TRUE),
              vectorized = list(map = TRUE),
              structured = list(map = TRUE))
## @knitr                                   
              })
  test(out, out.struct)              
               
## @knitr bigsum-input
  input.bigsum = to.dfs(keyval(rep(1, input.size), rnorm(input.size), vectorized=TRUE))
## @knitr 
  system.time({out = 
## @knitr bigsum                
    mapreduce(input.bigsum, 
              map  = function(k,v) keyval(1,v), 
              reduce = function(k, vv) keyval(k, sum(unlist(vv))),
              combine = TRUE)
## @knitr                                   
  })
  # user  system elapsed 
  # 272.156   7.503 253.903 
  #vec version
  system.time({out.vec = 
## @knitr bigsum-vec                
    mapreduce(input.bigsum,
              map  = function(k,v) keyval(1,sum(unlist(v)), vectorized = TRUE),
              reduce = function(k, vv) keyval(k, sum(unlist(vv))),
              combine = TRUE,
              vectorized = list(map = TRUE))
## @knitr                                   
              })
  # user  system elapsed 
  # 43.190   2.063  41.723 
  stopifnot(isTRUE(all.equal(from.dfs(out), from.dfs(out.vec), check.attributes=F)))
  
  system.time({out.struct = 
## @knitr bigsum-vec-struct                
    mapreduce(input.bigsum, 
              map  = function(k,v) keyval(1, sum(v), vectorized = TRUE), 
              reduce = function(k, vv) keyval(k, sum(vv)) , 
              combine = TRUE,
              vectorized = list(map = TRUE),
              structured = TRUE)
## @knitr                                   
              })
  stopifnot(isTRUE(all.equal(from.dfs(out), 
                             from.dfs(out.struct), 
                             check.attributes=F)))
  
## @knitr group-aggregate-input
  input.ga = to.dfs(keyval(1:input.size, rnorm(input.size), vectorized=TRUE))
## @knitr group-aggregate-functions
  group = function(k,v) unlist(k)%%100
  aggregate = function(x) sum(unlist(x))
## @knitr             
  system.time({out = 
## @knitr group-aggregate
    mapreduce(input.ga, 
              map = function(k,v) keyval(group(k,v), v),
              reduce = function(k, vv) keyval(k, aggregate(vv)),
              combine = TRUE)
## @knitr                                   
              })
  # user  system elapsed 
  # 280.608   6.838 250.180 
  system.time({out.vec = 
## @knitr group-aggregate-vec
    mapreduce(input.ga, 
              map = function(k,v) keyval(group(k,v), v, vectorized = TRUE),
              reduce = function(k, vv) keyval(k, aggregate(vv)),
              combine = TRUE,
              vectorized = list(map = TRUE))
## @knitr                                   
              })
  # user  system elapsed 
  # 114.444   3.720 110.314 
  stopifnot(isTRUE(all.equal(sort(unlist(values(from.dfs(out)))), 
                             sort(unlist(values(from.dfs(out.vec)))),
                             check.attributes=F)))
  #vec version, structured case
  system.time({out.struct = 
## @knitr group-aggregate-vec-struct
    mapreduce(input.ga, 
              map = function(k,v) keyval(group(k,v), v[,1], vectorized = TRUE),
              reduce = function(k, vv) keyval(k, aggregate(vv)),
              vectorized = list(map = TRUE),
              structured = TRUE)
## @knitr                                   
              })
  stopifnot(isTRUE(all.equal(sort(unlist(values(from.dfs(out)))), 
                             sort(unlist(values(from.dfs(out.struct)))),
                             check.attributes=F)))
}
