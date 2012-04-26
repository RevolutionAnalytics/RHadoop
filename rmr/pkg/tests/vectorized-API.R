#timings from macbook pro i7 2011, standalone CDH3, one core

#create input
input = to.dfs(list(keyval(rep(list(1), 10^6),1:10^6, vectorized=T)))



#pass through
system.time({out = mapreduce(input, map = function(k,v) keyval(k,v))})
# user  system elapsed 
# 179.345   4.671 179.111
#vec version
#vectorized.map says how many records to process in one map, default 1
system.time({out.vec = mapreduce(input,
                       map = function(k,v) keyval(k,v, vectorized = TRUE), 
                       vectorized = list(map = TRUE))})
# user  system elapsed 
# 46.370   1.830  42.669 



#filter
predicate = function(k,v) unlist(v)%%2 == 0
system.time({out = mapreduce(input,
          map = function(k,v) if(predicate(k,v)) keyval(k,v))})
# user  system elapsed 
# 124.485   3.472 120.507 
#vec version
system.time({out.vec = mapreduce(input, 
          map = function(k,v) {filter = predicate(k,v); 
                               keyval(k[filter], v[filter], vectorized = TRUE)},
          vectorized = list(map = TRUE))})
# user  system elapsed 
# 44.716   1.707  40.361 
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
#input for select
input.select = to.dfs(list(keyval(1:10^6, replicate(10^6, list(a=1,b=2,c=3), simplify=F), vectorized=T)))
select = function(v) v$b
select.vect = function(v) do.call(rbind,v)[,2] #names not preserved with current impl. of typedbytes
system.time({out = mapreduce(input.select,
          map = function(k,v) keyval(k, select(v)))})
# user  system elapsed 
# 175.964   3.874 169.601 
#vec version
system.time({out.vec = mapreduce(input.select,
          map = function(k,v) keyval(k, select.vect(v), vectorized = TRUE),
          vectorized = list(map = TRUE))})
# user  system elapsed 
# 38.363   1.790  32.683 
#vec version, structured case
mapreduce(input,
          map = function(k,v) keyval(k, v[,fields], vectorized = TRUE),
          vectorized = list(map = TRUE),
          structured = list(map = TRUE))



             
#bigsum
input.bigsum = to.dfs(list(keyval(rep(1, 10^6), rnorm(10^6), vectorized=T)))
system.time({out = mapreduce(input.bigsum, 
                             map  = function(k,v) keyval(1,v), 
                             reduce = function(k, vv) keyval(k, sum(unlist(vv))),
                             combine = TRUE)})
# user  system elapsed 
# 272.156   7.503 253.903 
#vec version
system.time({out.vec = mapreduce(input.bigsum,
                                 map  = function(k,v) keyval(1,sum(unlist(v)), vectorized = T),
                                 reduce = function(k, vv) keyval(k, sum(unlist(vv))),
                                 combine = TRUE,
                                 vectorized = list(map = TRUE))})
# user  system elapsed 
# 43.190   2.063  41.723 
#vec version, structured case
mapreduce(input, 
          map  = function(k,v) keyval(group(k,v),v, vectorized = TRUE), 
          reduce = function(k, vv) keyval(k, sum(vv)) , 
          vectorized = list(map = TRUE),
          structured = list(map = TRUE))
#or structured = c(T,T) or list(map = TRUE, reduce = TRUE)


             
#embarrassingly parallel
input.ep = to.dfs(list(keyval(1:10^6, rnorm(10^6), vectorized=T)))
group = function(k,v) unlist(k)%%100
aggregate = function(x) sum(unlist(x))
system.time({out = mapreduce(input.ep, 
                             map = function(k,v) keyval(group(k,v), v),
                             reduce = function(k, vv) keyval(k, aggregate(vv)),
                             combine = TRUE)})
# user  system elapsed 
# 280.608   6.838 250.180 
#vec version
system.time({out.vec = mapreduce(input.ep, 
                                 map = function(k,v) keyval(group(k,v), v, vectorized = TRUE),
                                 reduce = function(k, vv) keyval(k, aggregate(vv)),
                                 combine = TRUE,
                                 vectorized = list(map = TRUE))})
# user  system elapsed 
# 114.444   3.720 110.314 
#vec version, structured case
mapreduce(input, 
          map = function(k,v) keyval(group(k,v), v),
          reduce = function(k, vv) keyval(k, some.function(vv))
          vectorized.map = TRUE,
          structured = list(map = TRUE))
