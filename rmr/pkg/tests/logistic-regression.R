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

library(rmr)

## @knitr logistic.regression
logistic.regression = function(input, iterations, dims, alpha){
  plane = rep(0, dims)
  g = function(z) 1/(1 + exp(-z))
  for (i in 1:iterations) {
    gradient = from.dfs(mapreduce(input,
      map = function(k, v) keyval (1, v$y * v$x * g(-v$y * (plane %*% v$x))),
      reduce = function(k, vv) keyval(k, apply(do.call(rbind,vv),2,sum)),
      combine = T))
    plane = plane + alpha * gradient[[1]]$val }
  plane }
## @knitr end
out = list()
for (be in c("local", "hadoop")) {
  rmr.options.set(backend = be)
  ## create test set 
  set.seed(0)
  testdata = to.dfs(lapply (1:100, function(i) {eps = rnorm(1, sd =10) ; keyval(i, list(x = c(i,i+eps), y = 2 * (eps > 0) - 1))}))
  ## run 
  out[[be]] = logistic.regression(testdata, 3, 2, 0.05)
  ## max likelihood solution diverges for separable dataset, (-inf, inf) such as the above
}
stopifnot(isTRUE(all.equal(out[['local']], out[['hadoop']])))
