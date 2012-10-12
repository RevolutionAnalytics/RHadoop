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
                    keyval.length,
                    input.format, 
                    output.format, 
                    backend.parameters, 
                    verbose = verbose) {
  
  get.data =
    function(fname) {
     kv = from.dfs(fname, format = input.format)
     attr(kv$val, 'rmr.input') = fname
     kv}
  map.out = 
    apply.keyval(
      c.keyval(
        lapply(
          in.folder,
          get.data)),
      map,
      keyval.length)
  map.out = from.dfs(to.dfs(c.keyval(lapply(map.out, as.keyval))))
  reduce.helper = function(kk, vv) reduce(kk[1], vv)
  reduce.out = 
    if(!is.null(reduce))
      c.keyval(
        lapply(
          apply.keyval(
            map.out, 
            reduce.helper), 
          as.keyval))
    else
      map.out
  to.dfs(reduce.out, out.folder, format = output.format)}
