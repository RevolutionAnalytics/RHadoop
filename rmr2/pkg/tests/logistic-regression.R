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

 
## see spark implementation http://www.spark-project.org/examples.html
## see nice derivation here http://people.csail.mit.edu/jrennie/writing/lr.pdf

library(rmr2)

## @knitr logistic.regression-signature
logistic.regression = function(input, iterations, dims, alpha){

## @knitr logistic.regression-map
  lr.map =          
    function(dummy, M) {
      Y = M[,'y'] 
      X = M[,c("x1","x2")]
      keyval(1,
             Y * X * g(-Y * as.numeric(X %*% t(plane))))}
## @knitr logistic.regression-reduce
  lr.reduce =
    function(k, Z) keyval(k, t(as.matrix(apply(Z,2,sum))))
## @knitr logistic.regression-main
  plane = t(rep(0, dims))
  g = function(z) 1/(1 + exp(-z))
  for (i in 1:iterations) {
    gradient = 
      values(
        from.dfs(
          mapreduce(
            input,
            map = lr.map,     
            reduce = lr.reduce,
            combine = T)))
    plane = plane + alpha * gradient }
  plane }
## @knitr end

out = list()
test.size = 10^5
for (be in c("local", "hadoop")) {
  rmr.options(backend = be)
  ## create test set 
  set.seed(0)
  eps = rnorm(test.size)
  testdata = to.dfs(as.matrix(data.frame(x1 = 1:test.size, x2 = 1:test.size + eps, y = 2 * (eps > 0) - 1)))
  ## run 
  out[[be]] = logistic.regression(testdata, 3, 2, 0.05)
  ## max likelihood solution diverges for separable dataset, (-inf, inf) such as the above
}
stopifnot(isTRUE(all.equal(out[['local']], out[['hadoop']])))
