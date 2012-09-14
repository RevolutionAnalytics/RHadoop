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

freqs = 
  function(data, ...) {
    map.fc = 
      function(k,data) {
        fc = apply(data,2,function(x) aggregate(x,list(x),length)) 
        keyval(names(fc), fc)}
    
    reduce.fc =   
      function(colname, counts) {
        counts = do.call(rbind, counts)
        keyval(
          colname, 
          list(aggregate(counts$x, list(as.character(counts$Group.1)), sum)))}
    
    from.dfs(
      mapreduce(
        data, 
        map = map.fc, 
        reduce = reduce.fc,
        combine = T,
        ...))} 