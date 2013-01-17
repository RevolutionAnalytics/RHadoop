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

## @knitr kmeans-signature
kmeans.mr = 
  function(
    P, 
    num.clusters, 
    num.iter, 
    combine, 
    in.mem.combine) {
## @knitr kmeans-dist.fun
    dist.fun = 
      function(C, P) {
        apply(
          C,
          1, 
          function(x) 
            colSums((t(P) - x)^2))}
## @knitr kmeans.map
    kmeans.map = 
      function(., P) {
        nearest = {
          if(is.null(C)) 
            sample(
              1:num.clusters, 
              nrow(P), 
              replace = T)
          else {
            D = dist.fun(C, P)
            nearest = max.col(-D)}}
        if(!(combine || in.mem.combine))
          keyval(nearest, P) 
        else 
          keyval(nearest, cbind(1, P))}
## @knitr kmeans.reduce
    kmeans.reduce = {
      if (!(combine || in.mem.combine) ) 
        function(., P) 
          t(as.matrix(apply(P, 2, mean)))
      else 
        function(k, P) 
          keyval(
            k, 
            t(as.matrix(apply(P, 2, sum))))}
## @knitr kmeans-main-1  
    C = NULL
    for(i in 1:num.iter ) {
      C = 
        values(
          from.dfs(
            mapreduce(
              P, 
              map = kmeans.map,
              reduce = kmeans.reduce)))
      if(combine || in.mem.combine)
        C = C[, -1]/C[, 1]
      points(C, col = i + 1, pch = 19)
## @knitr kmeans-main-2
      if(nrow(C) < num.clusters) {
        C = 
          rbind(
            C,
            matrix(
              rnorm(
                (num.clusters - 
                   nrow(C)) * nrow(C)), 
              ncol = nrow(C)) %*% C) }}
        C}
## @knitr end

## sample runs
## 

out = list()

for(be in c("local", "hadoop")) {
  rmr.options(backend = be)
  set.seed(0)
## @knitr kmeans-data
  P = 
    do.call(
      rbind, 
      rep(
        list(
          matrix(
            rnorm(10, sd = 10), 
            ncol=2)), 
        20)) + 
    matrix(rnorm(200), ncol =2)
  x11()
  plot(P)
  points(P)
## @knitr end
  out[[be]] = 
## @knitr kmeans-run    
    kmeans.mr(
      to.dfs(P),
      num.clusters = 12, 
      num.iter = 5,
      combine = FALSE,
      in.mem.combine = FALSE)
## @knitr end
}

# would love to take this step but kmeans in randomized in a way that makes it hard to be completely reprodubile
# stopifnot(rmr2:::cmp(out[['hadoop']], out[['local']]))
