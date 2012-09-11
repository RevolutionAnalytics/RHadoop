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

library(quickcheck)
library(rmr2)

#has.rows
unit.test(
  function(x) {
    is.null(nrow(x)) == !rmr2:::has.rows(x)},
  list(tdgg.any()))

#all.have rows TODO
#rmr.length TODO

#keyval, keys.values
unit.test(
  function(x){
    k = x
    v = lapply(x, function(x) tdgg.any()())
    kv = keyval(k, v)
    identical(keys(kv), k) &&
      identical(values(kv), v)},
  list(tdgg.any()))

#NULL key case
unit.test(
  function(v){
    k = NULL
    kv = keyval(k, v)
    identical(keys(kv), k) &&
      identical(values(kv), v)},
  list(tdgg.any()))