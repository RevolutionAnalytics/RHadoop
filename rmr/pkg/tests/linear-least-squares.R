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

## @knitr linear.least.squares.swap
swap = function(x) list(x[[2]], x[[1]])
## @knitr end

## @knitr linear.least.squares.transpose.map
transpose.map = to.map(swap, identity)
## @knitr end

## @knitr linear.least.squares.transpose
transpose = function(input, output = NULL){
  mapreduce(input = input, output = output, map = transpose.map)
}
## @knitr end

#there is no need to go through the transpose and multiplication
#from.dfs(mapreduce(to.dfs(M), 
  #map = function (k,Mi) keyval(NULL, t(Mi)%*%Mi), 
  #reduce = function(k,XX) keyval(NULL, Reduce('+', XX)), combine = T))[[1]]$val

## @knitr linear.least.squares.mat.mult.map
mat.mult.map = function(i) function(k,v) keyval(k[[i]], list(pos = k, elem = v))
## @knitr end
## @knitr linear.least.squares.mat.mult
mat.mult = function(left, right, result = NULL) {
  mapreduce(
                input =
                equijoin(left.input = left, right.input = right,
                                 map.left = mat.mult.map(2),
                                 map.right = mat.mult.map(1), 
                                 reduce = function(k, vvl, vvr) 
                                   do.call(c, lapply(vvl, function(vl)
                                     lapply(vvr, function(vr) keyval(matrix(c(vl$pos[[1]], vr$pos[[2]]),nrow = 1), 
                                                                     vl$elem*vr$elem))))),
                output = result,
                reduce = to.reduce(identity, function(x) sum(unlist(x))))}
## @knitr end
## @knitr linear.least.squares.to.matrix
to.matrix = function(df) as.matrix(sparseMatrix(i=df$key[,1], j=df$key[,2], x=df$val[,1]))
## @knitr end
## @knitr linear.least.squares
linear.least.squares = function(X,y) {
  Xt = transpose(X)
  XtX = from.dfs(mat.mult(Xt, X), to.data.frame = TRUE)
  Xty = from.dfs(mat.mult(Xt, y), to.data.frame = TRUE)
  solve(to.matrix(XtX),to.matrix(Xty))}
## @knitr end

# test data

X = do.call(c, lapply(1:4, function(i) lapply(1:3, function(j) keyval(c(i,j), rnorm(1)))))

y = do.call(c, lapply(1:4, function(i) lapply(1:1, function(j) keyval(c(i,j), rnorm(1)))))

out = list()
for (be in c("local", "hadoop")) {
  rmr.options.set(backend = be)
  out[[be]] = linear.least.squares(to.dfs(X), to.dfs(y))}

stopifnot(isTRUE(all.equal(out[['local']], out[['hadoop']])))
