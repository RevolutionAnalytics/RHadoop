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

# the problem with the tests here is that they are more complex than the function they are meant to test
# or just a duplication. That's not very useful.

library(rmr2)
library(quickcheck)

#qw
unit.test(
  function(ss) {
    ss = paste("v", ss, sep = "")
    ss == eval(parse(text = paste("rmr2:::qw(", paste(ss, collapse = ","), ")")))},
  list(tdgg.character()))

# Make.single.arg
unit.test(
  function(l) {
    f = function(...) list(...)
    g = rmr2:::Make.single.arg(f)
    identical(do.call(f, l), g(l))},
  list(tdgg.list()))
                  
# Make.multi.arg
unit.test(
  function(l) {
    f = function(x) x
    g = rmr2:::Make.multi.arg(f)
    identical(do.call(g, l), f(l))},
  list(tdgg.list()))

# Make.single.or.multi.arg
unit.test(
  function(l, arity) {
    f = if(arity == "single") identity else c 
    g = rmr2:::Make.single.or.multi.arg(f, from = arity)
    identical(g(l), do.call(g, l))},
  list(tdgg.list(),
       tdgg.select(rmr2:::qw(single, multi))))

#%:% TODO
# all.predicate TODO

# make.fast.list TODO
# actually the function has been working forever, the test doesn't

# unit.test(
#   function(l){
#     fl = rmr2:::make.fast.list()
#     lapply(l, fl)
#     print(x=as.list(do.call(c, l)))
#     print(x=fl())
#     identical(as.list(do.call(c, l)), fl())},
#   list(tdgg.list(lambda=1, max.level=8)))
#     

#named.slice TODO
#catply TODO
#interleave TODO

