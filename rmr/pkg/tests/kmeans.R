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
 
kmeans.iter =
  function(points, distfun, ncenters = length(centers), centers = NULL) {
    from.dfs(mapreduce(input = points,
                         map = 
                           if (is.null(centers)) {
                             function(k,v) keyval(sample(1:ncenters,1),v)}
                           else {
                             function(k,v) {
                               distances = lapply(centers, function(c) distfun(c,v))
                               keyval(centers[[which.min(distances)]], v)}},
                         reduce = function(k,vv) keyval(NULL, apply(do.call(rbind, vv), 2, mean))))}

kmeans =
  function(points, ncenters, iterations = 10, distfun = function(a,b) norm(as.matrix(a-b), type = 'F'), plot = FALSE) {
    newCenters = kmeans.iter(points, distfun = distfun, ncenters = ncenters)
    if(plot) pdf = rmr:::to.data.frame(from.dfs(points))
    for(i in 1:iterations) {
      newCenters = lapply(rmr:::values(newCenters), unlist)
      newCenters = c(newCenters, lapply(sample(newCenters, ncenters-length(newCenters), replace = TRUE), function(x)x+rnorm(2,sd = 0.001)))
      if(plot) {
        png(paste(Sys.time(), "png", sep = "."))
        print(ggplot(data=pdf, aes(x=val1, y=val2) ) + 
          geom_jitter() +
          geom_jitter(data=rmr:::to.data.frame(newCenters), aes(x=X.1, y = X.2), color = "red"))
        dev.off()}
      newCenters = kmeans.iter(points, distfun, centers=newCenters)}
    newCenters
  }

## sample data, 12 cluster
## 
kmeans(
  to.dfs(
    lapply(
      1:10000,
      function(i) keyval(
        i, c(rnorm(1, mean = i%%3, sd = 0.01), 
             rnorm(1, mean = i%%4, sd = 0.01))))),
  12)
