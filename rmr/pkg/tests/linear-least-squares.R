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
                equijoin(left.input = left, right.input = right,
                                 map.left = mat.mult.map(2),
                                 map.right = mat.mult.map(1), 
                                 reduce = function(k, vvl, vvr) 
                                   do.call(c, lapply(vvl, function(vl)
                                     lapply(vvr, function(vr) keyval(c(vl$pos[[1]], vr$pos[[2]]), vl$elem*vr$elem))))),
                output = result,
                reduce = to.reduce(identity, function(x) sum(unlist(x))))}

to.matrix = function(df) as.matrix(sparseMatrix(i=df$rmr.key1, j=df$rmr.key2, x=df$V3))

linear.least.squares = function(X,y) {
  Xt = transpose(X)
  XtX = from.dfs(mat.mult(Xt, X), to.data.frame = TRUE)
  Xty = from.dfs(mat.mult(Xt, y), to.data.frame = TRUE)
  solve(to.matrix(XtX),to.matrix(Xty))}

# test data

X = do.call(c, lapply(1:4, function(i) lapply(1:3, function(j) keyval(c(i,j), rnorm(1)))))

y = do.call(c, lapply(1:4, function(i) lapply(1:1, function(j) keyval(c(i,j), rnorm(1)))))

out = list()
for (be in c("local", "hadoop")) {
  rmr.options.set(backend = be)
  out[[be]] = linear.least.squares(to.dfs(X), to.dfs(y))}

stopifnot(rmr:::cmp(out[['local']], out[['hadoop']]))
