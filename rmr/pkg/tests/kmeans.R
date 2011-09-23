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
  function(points, distfun, ncenters = dim(centers)[1], centers = NULL) {
    from.dfs(mapreduce(input = points,
                         map = 
                           if (is.null(centers)) {
                             function(k,v) keyval(sample(1:ncenters,1),v)}
                           else {
                             function(k,v) {
                               cat(is.null(centers), file = stderr())
                               distances = apply(centers, 1, function(c) distfun(c,v))
                               keyval(centers[which.min(distances),], v)}},
                         reduce = function(k,vv) keyval(NULL, apply(do.call(rbind, vv), 2, mean))),
             todataframe = T)}

kmeans =
  function(points, ncenters, iterations = 10, distfun = function(a,b) norm(as.matrix(a-b), type = 'F'), 
           plot = FALSE, fast = F) {
    if (fast) kmeans.iter = kmeans.iter.1
    newCenters = kmeans.iter(points, distfun = distfun, ncenters = ncenters)
    if(plot) pdf = rmr:::to.data.frame(do.call(c,values(from.dfs(points))))
    for(i in 1:iterations) {
      newCenters =  rbind(newCenters,
                          t(apply(newCenters[sample(1:dim(newCenters)[1], ncenters-dim(newCenters)[1], replace = T),],1,function(x)x + rnorm(2,sd = 0.1)))) #this is not general wrt dim, fix
      if(plot) {
        library(ggplot2)
        png(paste(Sys.time(), "png", sep = "."))
        print(ggplot(data = pdf, aes(x=V1, y=V2) ) + 
          geom_jitter() +
          geom_jitter(data = newCenters, aes(x = val.sum.sum1, y = val.sum.sum2), color = "red"))
        dev.off()}
      newCenters = kmeans.iter(points, distfun, centers = newCenters)}
    newCenters}

#points grouped many-per-record, assume centers few, loop over centers never points, use vectorised C primitives
kmeans.iter.1 = 
  function(points, distfun = NULL, ncenters = dim(centers)[1], centers = NULL) {
    fast.dist = function(pp, c) {
      squared.diffs = (t(t(pp) - c))^2
      sqrt(Reduce(`+`, lapply(1:dim(pp)[2], function(d) squared.diffs[,d])))}
    reduce = function(k, vv) {
      keyval(NULL, apply(do.call(rbind, vv), 2, sum))}
    newCenters = from.dfs(mapreduce(input = points,
                       map = 
                         if (is.null(centers)) {
                           function(k, v) {
                             v = do.call(rbind,v)
                             cat(apropos(".", where = T)[order(as.numeric(names(apropos(".", where = T))))][1:100], file = stderr())
                             centers = sample(1:ncenters, dim(v)[[1]], replace = TRUE)
                             lapply(1:ncenters, function(c) keyval(c, c(count = sum(centers == c), 
                                                                        apply(matrix(v[centers == c,], 
                                                                                     ncol = dim(v)[2]),2,sum))))}}
                         else {
                           function(k, v) {
                             v = do.call(rbind,v)
                             cat(apropos(".", where = T)[order(as.numeric(names(apropos(".", where = T))))][1:100], file = stderr())
                             dist.mat = apply(centers, 1, function(c) fast.dist(v, c))
                             closest.centers = as.data.frame(which(dist.mat == do.call(pmin,lapply(1:dim(dist.mat)[2], function(i)dist.mat[,i])), arr.ind=TRUE))
                             lapply(1:ncenters, function(c) keyval(c, c(count = sum(closest.centers$col == c),
                                                                        apply(matrix(v[subset(closest.centers, subset = col == c)[,'row'], ],
                                                                                     ncol = dim(v)[2]),2,sum))))}},
                       reduce =  reduce,
                       reduceondataframe = F,
                       combine = F),
             todataframe = T)
    newCenters = subset(newCenters, subset = val.count > 0)
    (newCenters/newCenters$val.count)[-1]}

## sample data, 12 cluster
## 
kmeans(
  to.dfs(
    lapply(
      1:10000,
      function(i) keyval(
        i, c(rnorm(1, mean = i%%3, sd = 0.1), 
             rnorm(1, mean = i%%4, sd = 0.1))))),
  12)
