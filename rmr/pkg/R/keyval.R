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
 
library(functional)

length.keyval = 
  function(kv) {
  if(is.null(nrow(keys(kv))))
    length(keys(kv))
  else
    nrow(keys(kv))}

keyval = function(k, v) {
  if (is.null(k) || (length.keyval(k) == length.keyval(v))
    list(key = k, val = v)
  else {
    if(length.keyval((k) == 1))
      list(key = k, val = list(v))
  else 
    stop("invalid key value combination", length.keyval(k), length.keyval(v))}}

all.predicate = function(x, P) all(sapply(x), P))
all.is.matrix = Curry(all.predicate, P = is.matrix )
all.is.data.frame = Curry(all.predicate, P = is.data.frame)

same.class = function(...) {
  args = list(...)
  if (length(args) == 1)
    classes = sapply(args[[1]], class)
  else
    classes = sapply(args, class)
  length(unique(classes)) == 1}

c.or.rbind = 
  Make.
    function(x) {
      if(all.is.matrix(x) || all.is.data.frame(x))
        do.call(rbind,x)
      else
        do.call(c,x)}

  
  keys(kv) = function(kv) kv$k
  values(kv) = function(kv) kv$v
  
  
  