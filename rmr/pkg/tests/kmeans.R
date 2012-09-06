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

library(rmr)

kmeans.mr = 
  function(P, num.clusters, num.iter) {
    dist.fun = 
      function(C, P) {
        apply(C,
              1, 
              function(x) matrix(rowSums((t(t(P) - x))^2), ncol = length(x)))}
  
    kmeans.map.1 = 
      function(k, P) {
        nearest = 
          if(is.null(C)) {
            sample(1:num.clusters, nrow(P), replace = T)}
        else {
          D = dist.fun(C, P)
          nearest = max.col(-D)}
        keyval(nearest, P) }
    
    kmeans.reduce.1 = function(x, P) {
      t(as.matrix(apply(P,2,mean)))}
    
    C = NULL
    for(i in 1:num.iter ) {
      C = 
        values(
          from.dfs(
            mapreduce(P, map = kmeans.map.1, reduce = kmeans.reduce.1))
      if(nrow(C) < 5) 
        C = matrix(rnorm(n.clust * nrow(C)), ncol = nrow(C)) %*% C }
    C}


## sample runs
## 

out = list()

for(be in c("local", "hadoop")) {
  rmr.options(backend = be)
  set.seed(0)
## @knitr kmeans.data
  input = 
    do.call(rbind, rep(list(matrix(rnorm(10, sd = 10), ncol=2)), 20)) + 
    matrix(rnorm(200), ncol =2)
## @knitr end
  out[[be]] = 
## @knitr kmeans.run    
    kmeans.mr(to.dfs(input), num.clusters  = 12, num.iter= 5)
## @knitr end
}
## @knitr kmeans.end

# would love to take this step but kmeans in randomized in a way that makes it hard to be completely reprodubile
#stopifnot(rmr:::cmp(out[['hadoop']], out[['local']]))
