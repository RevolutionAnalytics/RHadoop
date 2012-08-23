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

length.keyval = function(kv) rmr.length(values(kv))
  
keyval = 
  function(key, val = NULL) {
    if(missing(val)) list(key = NULL, val = key)
    else list(key = key, val = val)}

keys = function(kv) kv$k
values = function(kv) kv$v

is.keyval = 
  function(x) is.list(x) && length(x) == 2 && names(x) == qw(key, value)

as.keyval = 
  function(x) {
    if(is.keyval(x)) x
    else keyval(x)}

rmr.slice = 
  function(x, r) {
    if(has.rows(x))
      x[r,]
    else
      x[r]}

expand.keys = 
  function(kv)
    keyval(
      rmr.slice(
        if(is.null(keys(kv))) replicate(rmr.length(values(kv)), NULL, simplify = FALSE)
        else
          c.or.rbind(replicate(ceiling(rmr.length(values(kv))/rmr.length(keys(kv))),
                               keys(kv),
                               simplify = FALSE)),
        1:rmr.length(values(kv))),
      values(kv))

slice.keyval = 
  function(kv, r)
    keyval(rmr.slice(expand.keys(keys(kv), values(kv)), r),
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
  function(kvs) {
    kvs = lapply(kvs, expand.keys)
    vv = lapply(kvs, values)
    kk = lapply(kvs, keys)
    keyval(c.or.rbind(kk), c.or.rbind(vv))})
  
split.keyval = function(kv, size = 1000) {
  k = keys(kv)
  v = values(kv)
  split.v =
    if(has.rows(v))
      split.data.frame
  else split
  split.k =
    if(has.rows(k))
      split.data.frame
  else split
  if(is.null(k)) {
    k =  ceiling(1:rmr.length(v)/size)
    keyval(NULL,
           split(v, k))}
  else {
    ind = 
      if(is.list(k) && !is.data.frame(k))
        sapply(k, digest)
      else
        k
    keyval(unname(split.k(k, ind)), 
           unname(split.v(v, ind)))}}  

apply.keyval = 
  function(kv, FUN, split.size = 1000) {
    kvs = split.keyval(kv, split.size)
    mapply(FUN, keys(kvs), values(kvs), SIMPLIFY = FALSE)}