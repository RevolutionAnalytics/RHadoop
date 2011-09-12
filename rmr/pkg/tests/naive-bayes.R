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

# this is just an example, not part of a math library
# matrix A_{ij} representation is a list of keyval(c(i,j), A_{ij})
# vectors are column matrices

## input format is keyval(NULL, list(x=c(x1, ..., xn), y = y)

##library(rmr)

##naive.bayes = function(input, output = NULL) {
##  mapreduce(input = input, output = output,
##            map = function(k, v) c(lapply(1:length(v$x) function(i) keyval(c(i, v$x[i], v$y),1)),
##                                   lapply),
##            reduce = function(k, vv) keyval(k, sum(unlist(vv))),
##            combiner = T)
##}