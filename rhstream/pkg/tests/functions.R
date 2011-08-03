library(RevoHStream)

##main function, should be factored out one day

test = function(property,times, ...) {
  lapply(1:100, function(i) {
    args = lapply(list(...), function(a) do.call(a, list()))
    if(!do.call(property, args)) stop(deparse(list(property, args)))})}

## random data structure generators
rnumericlist = function(lambda) function() lapply(1:rpois(1,lambda), runif(1))

rnumerickeyvallist = function(lambda) function() lapply(1:rpois(1,lambda), function(i) keyval(runif(2)))

test(function(kvl) all.equal(kvl, apply(cbind(getKeys(kvl), getValues(kvl)),1,keyval)), 10, rnumerickeyvallist(10))