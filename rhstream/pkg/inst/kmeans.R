rhkmeansiter =
  function(points, distance, ncenters = length(centers), centers = NULL, summaryfun) {
    centerfile = tempfile()
    revoMapReduce(input = points,
             output= centerfile,
             map = function(k,v) {
               if (is.null(centers)) {
                 keyval(sample(1:ncenters,1),v)}
               else {
                 distances = lapply(centers, function(c) distance(c,v))
                 keyval(centers[[which.min(distances)]], v)}},
             reduce = function(k,vv) keyval(NULL, apply(do.call(rbind, vv), 2, mean)))
    centers = rhread(centerfile)
  }

rhkmeans =
  function(points, ncenters, distance = function(a,b) norm(as.matrix(a-b), type = 'F'), summaryfun = mean) {
    newCenters = rhkmeansiter(points, distance = distance, ncenters = ncenters, summaryfun = summaryfun)
    for(i in 1:20) {
      newCenters = lapply(RevoHStream:::getValues(newCenters), unlist)
      newCenters = rhkmeansiter(points, dist, centers=newCenters)}
    newCenters
  }

## sample data, 12 clusters
## clustdata = lapply(1:100, function(i) keyval(i, c(rnorm(1, mean = i%%3, sd = sd), rnorm(1, mean = i%%4, sd = sd))))
## call with
## rhwrite(clustdata, "/tmp/clustdata")
## rhkmeans ("/tmp/clustdata", 12)
