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

mr.local = function(map, 
                    reduce, 
                    combine, 
                    in.folder, 
                    out.folder, 
                    profile.nodes, 
                    input.format, 
                    output.format, 
                    vectorized.map,
                    backend.parameters, 
                    verbose = verbose) {
  if(is.null(reduce)) reduce = function(k, vv) lapply(vv, function(v) keyval(k, v))
  
  get.data =
    function(fname) {
      in.data = split.keyval(from.dfs(fname, format = input.format), vectorized.map)
      lapply(in.data, function(rec) {attr(rec$val, 'rmr.input') = fname; rec})}
  map.out = 
    lapply(
      catply(
        in.folder,
        get.data),
      function(kv) map(keys(kv), values(kv)))
  map.out = from.dfs(to.dfs(c.keyval(lapply(map.out, as.keyval))))
  reduce.out = tapply(X = map.out, 
                      INDEX = sapply(map.out, function(x) digest(keys(x))), 
                      FUN = function(x) reduce(keys(x), 
                                               values(x)), 
                      simplify = FALSE)
  names(reduce.out) = replicate(n=length(names(reduce.out)), "")
  to.dfs(c.keyval(lapply(as.keyval, reduce.out)), out.folder, format = output.format)}
