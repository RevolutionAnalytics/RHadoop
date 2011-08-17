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

 
##input is X: k = (i,j) , v = x_{ij}
##

swap = function(x) list(x[[2]], x[[1]])

transposeMap = mkMap(swap, identity)

# test matrix
# X = do.call(c, lapply(1:3, function(i) lapply(1:4, function(j) keyval(c(i,j), 10*1+j))))
rhTranspose = function(input, output = NULL){
  revoMapReduce(input = input, output = output, map = transposeMap)
}

matMulMap = function(i) function(k,v) keyval(k[[i]], list(pos = k, elem = v))

rhMatMult = function(left, right, result = NULL) {
  revoMapReduce(
                input =
                rhRelationalJoin(leftinput = left, rightinput = right,
                                 map.left.keyval = matMulMap(2),
                                 map.right.keyval = matMulMap(1), 
                                 reduce.keyval = function(k, vl, vr) keyval(c(vl$pos[[1]], vr$pos[[2]]), vl$elem*vr$elem)),
                output = result,
                map = mkMap(identity),
                reduce = mkReduce(identity, function(x) sum(unlist(x))))}

to.matrix = function(df) as.matrix(sparseMatrix(i=df$key1, j=df$key2, x=df$val))

rhLinearLeastSquares = function(X,y) {
  library(Matrix)
  XtX = rhread(rhMatMult(rhTranspose(X), X), todataframe = TRUE)
  Xty = rhread(rhMatMult(rhTranspose(X), y), todataframe = TRUE)
  solve(to.matrix(XtX),to.matrix(Xty))}
