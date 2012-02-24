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


library(Matrix)
library(rmr)

swap = function(x) list(x[[2]], x[[1]])

transpose.map = to.map(swap, identity)

transpose = function(input, output = NULL){
  mapreduce(input = input, output = output, map = transpose.map)
}

mat.mult.map = function(i) function(k,v) keyval(k[[i]], list(pos = k, elem = v))

mat.mult = function(left, right, result = NULL) {
  mapreduce(
                input =
                equijoin(leftinput = left, rightinput = right,
                                 map.left = mat.mult.map(2),
                                 map.right = mat.mult.map(1), 
                                 reduce = function(k, vvl, vvr) 
                                   do.call(c, lapply(vvl, function(vl)
                                     lapply(vvr, function(vr) keyval(c(vl$pos[[1]], vr$pos[[2]]), vl$elem*vr$elem))))),
                output = result,
                reduce = to.reduce(identity, function(x) sum(unlist(x))))}

`%x%` = mat.mult

to.matrix = function(df) as.matrix(sparseMatrix(i=df$key1, j=df$key2, x=df$val))

weighted.linear.least.squares = function(X, y, W) {
  Xt = transpose(X)
  XtWX = from.dfs(Xt %x% W %x% X, todataframe = TRUE)
  XtWy = from.dfs(Xt %x% W %x% y, todataframe = TRUE)
  solve(to.matrix(XtWX),to.matrix(XtWy))}

# test data
X = do.call(c, lapply(1:4, function(i) lapply(1:3, function(j) keyval(c(i,j), rnorm(1)))))
y = do.call(c, lapply(1:4, function(i) lapply(1:1, function(j) keyval(c(i,j), rnorm(1)))))
W = lapply(1:4, function(i)  keyval(c(i,i), rnorm(1)))

out = list()
for (be in c("local", "hadoop")) {
  rmr.backend(be)
  out[[be]] = weighted.linear.least.squares(to.dfs(X), to.dfs(y), to.dfs(W))}

stopifnot(rmr:::cmp(out[['local']], out[['hadoop']]))
