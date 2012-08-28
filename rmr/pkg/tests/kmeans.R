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
  function(P, n.clust, n.iter, plot = F) {
    dist.fun = 
      function(C, P) {
        apply(C,
              1, 
              function(x) matrix(rowSums((t(t(P) - x))^2), ncol = length(x)))}
    
    kmeans.map.1 = 
      function(k, P) {
        nearest = 
          if(is.null(C)) {
            sample(1:n.clust, nrow(P), replace = T)}
        else {
          D = dist.fun(C, P)
          nearest = max.col(-D)}
        if(plot) print(points(P, col = nearest, pch = 19, cex = .5))
        Sys.sleep(.1)
        keyval(nearest, P) }
    
    kmeans.reduce.1 = function(x, P) {
      t(as.matrix(apply(P,2,mean)))}
    
    C = NULL
    for(i in 1:n.iter ) {
      C = 
        from.dfs(
          mapreduce(P, map = kmeans.map.1, reduce = kmeans.reduce.1))$val
      if(nrow(C) < 5) C = rbind(C , matrix(rnorm(2*(n.clust-nrow(C))),ncol = 2))}
    C}


## @knitr kmeans.iter
kmeans.iter =
  function(points, distfun, ncenters = dim(centers)[1], centers = NULL) {
    from.dfs(mapreduce(input = points,
                         map = 
                           if (is.null(centers)) {
                             function(k,v) keyval(sample(1:ncenters,1),v)}
                           else {
                             function(k,v) {
                               distances = apply(centers, 1, function(c) distfun(c,v))
                               keyval(centers[which.min(distances),], v)}},
                         reduce = function(k,vv) keyval(NULL, matrix(apply(do.call(rbind, vv), 2, mean), nrow=1))),
             structured = T)$val}
## @knitr end

#points grouped many-per-record something like 1000 should give most perf improvement, 
#assume centers fewer than points, loop over centers never points, use vectorised C primitives
#don't take averages right away, do sums and counts first which are associative and commutative
#then take the ratio. This allows using a combiner and other early data reduction step

## @knitr kmeans.iter.fast.signature
kmeans.iter.fast = 
  function(points, distfun, ncenters = dim(centers)[1], centers = NULL) {
## @knitr kmeans.iter.fast.list.to.matrix
    list.to.matrix = function(l) do.call(rbind,l) 
## @knitr kmeans.iter.fast.mapreduce 
    newCenters = from.dfs(
      mapreduce(
        input = points,
## @knitr kmeans.iter.fast.map.first
        map = 
          if (is.null(centers)) {
            function(k, v) {
              v = cbind(1, v)
              ##pick random centers
              centers = sample(1:ncenters, dim(v)[[1]], replace = TRUE) 
              clusters = unclass(by(v,centers,function(x) apply(x,2,sum)))
              lapply(names(clusters), function(cl) keyval(as.integer(cl), clusters[[cl]]))}}
## @knitr kmeans.iter.fast.rest
          else {
            function(k, v) {
              dist.mat = apply(centers, 1, function(x) distfun(v, x))
              closest.centers = as.data.frame(
                which( #this finds the index of the min row by row, but one can't loop on the rows so we must use pmin
                  dist.mat == do.call(
                    pmin,
                    lapply(1:dim(dist.mat)[2], 
                           function(i) dist.mat[,i])), 
                  arr.ind=TRUE))
              closest.centers[closest.centers$row,] = closest.centers
              v = cbind(1, v)
              #group by closest center and sum up, kind of an early combiner
              clusters = unclass(by(v,closest.centers$col,function(x) apply(x,2,sum))) 
              lapply(names(clusters), function(cl) keyval(as.integer(cl), clusters[[cl]]))}},
## @knitr kmeans.iter.fast.reduce
        reduce = function(k, vv) {
               keyval(k, matrix(apply(list.to.matrix(vv), 2, sum), nrow = 1))},
## @knitr kmeans.iter.fast.options
        combine = T),
      structured = T)
## @knitr end    
    ## convention is iteration returns sum of points not average and first element of each sum is the count
## @knitr kmeans.iter.fast.newcenters    
    newCenters = cbind(newCenters$key, newCenters$val)
    newCenters = newCenters[newCenters[,2] > 0, -1]
    (newCenters/newCenters[,1])[,-1]}
## @knitr end

## @knitr kmeans.fast.dist
fast.dist = function(yy, x) { #compute all the distances between x and rows of yy
      squared.diffs = (t(t(yy) - x))^2
      ##sum the columns, take the root, loop on dimension
      sqrt(Reduce(`+`, lapply(1:dim(yy)[2], function(d) squared.diffs[,d])))}
## @knitr end

## @knitr kmeans.control
kmeans =
  function(points, ncenters, iterations = 10, distfun = NULL, 
           plot = FALSE, fast = F) {
    if(is.null(distfun)) 
      distfun = 
        if (!fast) function(a,b) norm(as.matrix(a-b), type = 'F')
        else fast.dist  
    if (fast) kmeans.iter = kmeans.iter.fast
    newCenters = kmeans.iter(points, distfun, ncenters = ncenters)
    for(i in 1:iterations)
      newCenters = kmeans.iter(points, distfun, centers = newCenters)
    newCenters}
## @knitr end

## sample runs
## 

out = list()
out.fast = list()

for(be in c("local", "hadoop")) {
  rmr.options.set(backend = be)
  set.seed(0)
## @knitr kmeans.data
  input = to.dfs(lapply(1:1000, function(i) keyval(NULL, c(rnorm(1, mean = i%%3, sd = 0.1), 
                                                         rnorm(1, mean = i%%4, sd = 0.1)))))
## @knitr end
  out[[be]] = 
## @knitr kmeans.run    
    kmeans(input, 12, iterations = 5)
## @knitr end
  set.seed(0)
## @knitr kmeans.data.fast
  recsize = 1000
  input = to.dfs(lapply(1:100, 
                        function(i) keyval(NULL, cbind(sample(0:2, recsize, replace = T) + rnorm(recsize, sd = .1),     
                                                       sample(0:3, recsize, replace = T) + rnorm(recsize, sd = .1)))))
## @knitr end
  out.fast[[be]] = 
## @knitr kmeans.run.fast
    kmeans(input, 12, iterations = 5, fast = T)}
## @knitr kmeans.end

# would love to take this step but kmeans in randomized in a way that makes it hard to be completely reprodubile
#stopifnot(rmr:::cmp(out[['hadoop']], out[['local']]))
#stopifnot(rmr:::cmp(out.fast[['hadoop']], out.fast[['local']]))
