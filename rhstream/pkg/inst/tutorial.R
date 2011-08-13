small.ints = 1:10
lapply(small.ints, function(x) x^2)

small.ints = rhwrite(1:10)
revoMapReduce(input=small.ints, map = function(k,v) keyval(k^2))

rhread(revoMapReduce(input=small.ints, map = function(k,v) keyval(k^2)))
