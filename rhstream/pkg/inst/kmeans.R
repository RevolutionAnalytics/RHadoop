rhkmeansiter =
  function(points, distfun, ncenters = length(centers), centers = NULL, summaryfun) {
    rhread(revoMapReduce(input = points,
                         map = 
                           if (is.null(centers)) {
                             function(k,v) keyval(sample(1:ncenters,1),v)}
                           else {
                             function(k,v) {
                               distances = lapply(centers, function(c) distfun(c,v))
                               keyval(centers[[which.min(distances)]], v)}},
                         reduce = function(k,vv) keyval(NULL, apply(do.call(rbind, vv), 2, mean))))}

rhkmeans =
  function(points, ncenters, iterations = 10, distfun = function(a,b) norm(as.matrix(a-b), type = 'F'), summaryfun = mean) {
    newCenters = rhkmeansiter(points, distfun = distfun, ncenters = ncenters, summaryfun = summaryfun)
    for(i in 1:iterations) {
      newCenters = lapply(RevoHStream:::getValues(newCenters), unlist)
      newCenters = rhkmeansiter(points, distfun, centers=newCenters)}
    newCenters
  }

## sample data, 12 clusters
## clustdata = lapply(1:100, function(i) keyval(i, c(rnorm(1, mean = i%%3, sd = 0.01), rnorm(1, mean = i%%4, sd = 0.01))))
## call with
## rhwrite(clustdata, "/tmp/clustdata")
## rhkmeans ("/tmp/clustdata", 12)
