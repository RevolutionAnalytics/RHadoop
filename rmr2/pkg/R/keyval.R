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

has.rows = function(x) !is.null(nrow(x))
all.have.rows = Curry(all.predicate, P = has.rows)

rmr.length = 
  function(x) if(has.rows(x)) nrow(x) else length(x)

length.keyval = 
  function(kv) 
    max(rmr.length(keys(kv)), 
        rmr.length(values(kv)))
  
keyval = 
  function(key, val = NULL) {
    if(missing(val)) list(key = NULL, val = key)
    else list(key = key, val = val)}

keys = function(kv) kv$key
values = function(kv) kv$val

is.keyval = 
  function(x) 
    is.list(x) && 
      length(x) == 2 && 
      !is.null(names(x)) && 
      all(names(x) == qw(key, val))


as.keyval = 
  function(x) {
    if(is.keyval(x)) x
    else keyval(x)}

rmr.slice = 
  function(x, r) {
    if(has.rows(x))
      x[r, , drop = FALSE]
    else
      x[r]}

rmr.recycle = 
  function(x,y) {
    recycle.length = 
      function(z) {
        if(is.null(z) || rmr.length(z) == 0) 1 
        else rmr.length(z) }
    lx = recycle.length(x)
    ly = recycle.length(y)
    if(lx == ly) x
    else
      rmr.slice(
        c.or.rbind(
          rep(list(x),
              ceiling(ly/lx))),
        1:max(ly, lx))}

recycle.keyval =
  function(kv) {
    k = keys(kv)
    v = values(kv)
    keyval(
      rmr.recycle(k, v),
      rmr.recycle(v, k))}

slice.keyval = 
  function(kv, r) {
    kv = recycle.keyval(kv)
    keyval(rmr.slice(keys(kv), r),
           rmr.slice(values(kv), r))}

c.or.rbind = 
  Make.single.or.multi.arg(
    function(x) {
      if(has.rows(x[[1]]))        
        do.call(rbind,x)
      else
        do.call(c,x)})

c.keyval = 
  Make.single.or.multi.arg(
  function(kvs) {
    kvs = lapply(kvs, recycle.keyval)
    vv = lapply(kvs, values)
    kk = lapply(kvs, keys)
    keyval(c.or.rbind(kk), c.or.rbind(vv))})
  
rmr.split = 
  function(x, ind) {
    spl = if(has.rows(x)) split.data.frame else split
    spl(x,ind)}

split.keyval = function(kv, size) {
  k = keys(kv)
  v = rmr.recycle(values(kv), k)
  if(is.null(k)) {
    k =  ceiling(1:rmr.length(v)/size)
    recycle.keyval(
      keyval(list(NULL),
             unname(rmr.split(v, k))))}
  else {
    ind = 
      if(is.list(k) && !is.data.frame(k))
        sapply(k, digest)
      else
        k
    keyval(lapply(unname(rmr.split(k, ind)), unique), 
           unname(rmr.split(v, ind)))}}  

apply.keyval = 
  function(
    kv, 
    FUN, 
    split.size = 
      stop("Must specify key when using keyval in map and combine functions")) {
    kvs = split.keyval(kv, split.size)
    mapply(FUN, keys(kvs), values(kvs), SIMPLIFY = FALSE)}