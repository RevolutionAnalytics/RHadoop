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
# X = do.call(c, lapply(1:2, function(i) lapply(1:2, function(j) keyval(c(i,j), rnorm(1)))))
rhTranspose = function(input, output = NULL){
  warning("This examples is neither finished nor tested, please do not run")
  revoMapReduce(input = input, output = output, map = transposeMap)
}

matMulMap = function(i) function(k,v) keyval(k[[i]], list(pos = k, elem = v))

rhMatMult = function(left, right, result = NULL) {
  warning("This examples is neither finished nor tested, please do not run")
  revoMapReduce(
                input =
                rhRelationalJoin(leftinput = left, rightinput = right,
                                 map.left.keyval = matMulMap(2),
                                 map.right.keyval = matMulMap(1), 
                                 reduce.keyval = function(k, vl, vr) keyval(c(vl$pos[[1]], vr$pos[[2]]), vl$elem*vr$elem)),
                output = result,
                map = mkMap(identity),
                reduce = mkReduce(identity, function(x) sum(unlist(x))))}

rhLinearLeastSquares = function(X,y) {
  warning("This examples is neither finished nor tested, please do not run")
  XtX = rhread(rhMatMult(rhTranspose(X), X))
  Xty = rhread(rhMatMul(rhTranspose(X), y))
  solve(XtX,Xty)}
