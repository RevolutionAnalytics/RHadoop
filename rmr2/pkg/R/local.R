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
                    vectorized.reduce,
                    in.folder, 
                    out.folder, 
                    profile.nodes, 
                    keyval.length,
                    rmr.install,
                    rmr.update,
                    input.format, 
                    output.format, 
                    in.mem.combine,
                    backend.parameters, 
                    verbose = verbose) {
  
  get.data =
    function(fname) {
     kv = from.dfs(fname, format = input.format)
     attr(kv$val, 'rmr.input') = fname
     kv}
  map.out = 
    c.keyval(
      do.call(
        c,    
        lapply(
          in.folder,
          function(fname)
            apply.keyval(
              get.data(fname),
              function(k, v) {
                Sys.setenv(map_input_file = fname)
                as.keyval(map(k, v))}, 
            keyval.length))))
  map.out = from.dfs(to.dfs(map.out))
  reduce.helper = 
    function(kk, vv) as.keyval(reduce(rmr.slice(kk,1), vv))
  reduce.out = { 
    if(!is.null(reduce)){
      if(!vectorized.reduce){
        c.keyval(
          apply.keyval(
            map.out, 
            reduce.helper))}
      else{
        as.keyval(
          reduce(
            lapply(keys(map.out), Curry(rmr.slice, r = 1)), 
            values(map.out)))}}    
    else
      map.out}
  to.dfs(reduce.out, out.folder, format = output.format)}
