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

has.rows = function(x) !is.null(nrow(x))
all.have.rows = Curry(all.predicate, P = has.rows)

rmr.length = 
  function(x) if(has.rows(x)) nrow(x) else length(x)

length.keyval =   function(kv) rmr.length(keys(kv))
  
keyval = function(k = NULL, v) {
  if (is.null(k) || (length.keyval(k) == length.keyval(v))
    list(key = k, val = v)
  else {
    if(length.keyval((k) == 1))
      list(key = k, val = list(v))
  else 
    stop("invalid key value combination", length.keyval(k), length.keyval(v))}}


keys(kv) = function(kv) kv$k
values(kv) = function(kv) kv$v

rmr.slice = 
  function(x, r) {
    if(has.rows(x))
      x[r,]
    else
      x[r]}

slice.keyval = function(kv, r)
  keyval(rmr.slice(keys(kv), r)
         rmr.slice(values(kv), r))

c.or.rbind = 
  Make.single.or.multi.arg(
    function(x) {
      if(all.have.rows(x))        
        do.call(rbind,x)
      else
        do.call(c,x)})

c.keyval = 
  Make.single.or.multi.arg(
  function(x) {
    keyval(c.or.rbind(lapply(x, keys), c.or.rbind(lapply(values(x)))})
  
split.keyval = function(kv) {
  if(has.rows(values(kv)))
    split = split.data.frame
  if(is.null(keys(kv))) {
    lapply(
      split(
        values(kv),
        ceiling(1:nrow(values(kv))/rmr.options.get("vectorized.nrow"))),
      function(v) keyval(NULL, V))}
  else
    lapply(1:rmr.length(kv), function(i) slice.keyval(kv, i))}

  
  
  