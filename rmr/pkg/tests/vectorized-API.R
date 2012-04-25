#timings from macbook pro i7 2011, standalone CDH3, one core

#create input
input = to.dfs(list(keyval(rep(list(1), 5 * 10^5),1:(5*10^5), vectorized=T)))



#pass through
system.time({out = mapreduce(input, map = function(k,v) keyval(k,v))})
# user  system elapsed 
# 73.904   1.303  69.421 
#vec version
#vectorized.map says how many records to process in one map, default 1
system.time({out.vec = mapreduce(input,
                       map = function(k,v) keyval(k,v, vectorized = TRUE), 
                       vectorized = list(map = TRUE))})
# user  system elapsed 
# 9.920   0.870   6.345 



#filter
predicate = function(k,v) unlist(v)%%2 == 0
system.time({out = mapreduce(input,
          map = function(k,v) if(predicate(k,v)) keyval(k,v))})
# user  system elapsed 
# 49.020   1.052  61.693 
#vec version
system.time({out.vec = mapreduce(input, 
          map = function(k,v) {filter = predicate(k,v); 
                               keyval(k[filter], v[filter], vectorized = TRUE)},
          vectorized = list(map = TRUE))})
#vec version, structured case
#structured says to convert list to data frame. Fails if not possible. If TRUE,
#it means both map and reduce, or it is a named vector or list(map = TRUE, reduce = FALSE)
#default both FALSE
mapreduce(input, 
          map = function(k,v) {filter = predicate(k,v); 
                               keyval(k[filter,], v[filter,], vectorized = TRUE)},
          vectorized = list(map = TRUE),
          structured = list(map = TRUE))


             
#select
system.time({out = mapreduce(input,
          map = function(k,v) keyval(k, select(v)))})
#vec version
system.time({out.vec = mapreduce(input,
          map = function(k,v) keyval(k, select(v), vectorized = TRUE),
          vectorized = list(map = TRUE))})
#vec version, structured case
mapreduce(input,
          map = function(k,v) keyval(k, v[,fields], vectorized = TRUE),
          vectorized = list(map = TRUE),
          structured = list(map = TRUE))



             
#bigsum
system.time({out =mapreduce(input, 
          map  = function(k,v) keyval(group(k,v),v), 
          reduce = function(k, vv) keyval(k, sum(unlist(vv))))})
#vec version
system.time({out.vec = mapreduce(input, 
          map  = function(k,v) keyval(group(k,v),v, vectorized = TRUE), 
          reduce = function(k, vv) keyval(k, sum(unlist(vv))), 
          vectorized = list(map = TRUE))})
#vec version, structured case
mapreduce(input, 
          map  = function(k,v) keyval(group(k,v),v, vectorized = TRUE), 
          reduce = function(k, vv) keyval(k, sum(vv)) , 
          vectorized = list(map = TRUE),
          structured = list(map = TRUE))
#or structured = c(T,T) or list(map = TRUE, reduce = TRUE)


             
#embarrassingly parallel
system.time({out = mapreduce(input, 
          map = function(k,v) keyval(group(k,v), v),
          reduce = function(k, vv) keyval(k, aggregate(vv)))})
#vec version
system.time({out.vec = mapreduce(input, 
          map = function(k,v) keyval(group(k,v), v, vectorized = TRUE),
          reduce = function(k, vv) keyval(k, aggregate(vv)),
          vectorized = list(map = TRUE))})
#vec version, structured case
mapreduce(input, 
          map = function(k,v) keyval(group(k,v), v),
          reduce = function(k, vv) keyval(k, some.function(vv))
          vectorized.map = TRUE,
          structured = list(map = TRUE))
