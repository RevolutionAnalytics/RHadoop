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

#string

qw = function(...) as.character(match.call())[-1]


#functional

Make.single.arg = 
  function(f)
    function(x) do.call(f,x)

Make.multi.arg = 
  function(f)
    function(...) f(list(...))

Make.single.or.multi.arg = function(f, from = qw(single, multi)) {
  from = match.arg(from)
  if (from == "single") {
    f.single = f
    f.multi = Make.multi.arg(f)}
  else {
    f.single = Make.single.arg(f)
    f.multi = f}
  function(...) {
    args = list(...)
    if(length(args) == 1)
      f.single(args[[1]])
    else
      f.multi(...)}}
  

`%:%` = function(f,g) function(...) do.call(f, g(...))
