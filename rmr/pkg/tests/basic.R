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

#qw

rmr:::unit.test(
  function(ss) {
    ss = paste("v", ss, sep = "")
    ss == eval(parse(text = paste("rmr:::qw(", paste(ss, collapse = ","), ")")))},
  list(rmr:::tdgg.character()))

# Make.single.arg

rmr:::unit.test(
  function(l) {
    f = function(...) list(...)
    g = rmr:::Make.single.arg(f)
    identical(do.call(f, l), g(l))},
  list(rmr:::tdgg.list()))
                  
# Make.multi.arg
rmr:::unit.test(
  function(l) {
    f = function(x) x
    g = rmr:::Make.multi.arg(f)
    identical(do.call(g, l), f(l))},
  list(rmr:::tdgg.list()))

# Make.single.or.multi.arg
rmr:::unit.test(
  function(l) {
    f = function(x) x
    g = rmr:::Make.single.or.multi.arg(f, from = "single")
    identical(g(l), do.call(g, l))},
  list(rmr:::tdgg.list(),
       rmr:::tdgg.list()))


rmr:::unit.test(
  function(l) {
    f = function(...) c(...)
    g = rmr:::Make.single.or.multi.arg(f, from = "multi")
    identical(f(l), do.call(f, l)),
  list(rmr:::tdgg.list(),
       rmr:::tdgg.list()))