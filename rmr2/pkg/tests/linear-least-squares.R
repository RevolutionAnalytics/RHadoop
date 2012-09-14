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

library(rmr2)

## @knitr LLS-data
X = to.dfs(matrix(rnorm(2000), ncol = 10))
y = as.matrix(rnorm(200))
## @knitr LLS-sum
Sum = function(k, YY) keyval(1, list(Reduce('+', YY)))
## @knitr LLS-XtX
XtX = 
  values(
    from.dfs(
      mapreduce(
        input = X,
        map = 
          function(k, Xi) 
            keyval(1, list(t(Xi) %*% Xi)),
        reduce = Sum,
        combine = TRUE)))[[1]]
## @knitr LLS-Xty
Xty = 
  values(
    from.dfs(
      mapreduce(
        input = X,
        map = function(k, Xi)
          keyval(1, list(t(Xi) %*% y)),
        reduce = Sum,
        combine = TRUE)))[[1]]
## @knitr LLS-solve
solve(XtX, Xty)
## @knitr end