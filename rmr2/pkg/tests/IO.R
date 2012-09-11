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

## test for typed bytes read/write
library(quickcheck)
library(rmr2)


unit.test(
  function(l) {
    l = rapply(l, how = 'replace', 
               function(x){
                 if(is.null(x)) list()
                 else as.list(x)})
    isTRUE(all.equal(l, 
                     rmr:::typed.bytes.reader(rmr:::typed.bytes.writer(l), length(l) + 5)$objects, 
                     check.attributes = FALSE))},
  generators = list(tdgg.list()))
