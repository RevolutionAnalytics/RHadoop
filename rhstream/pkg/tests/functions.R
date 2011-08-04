library(RevoHStream)

##main function, should be factored out one day

unittest = function(property, generators, iterations=10, condition = function(...) T) {
  set.seed(0)
  lapply(1:100, function(i) {
    args = lapply(generators, function(a) a())
    if(do.call(condition, args) && !do.call(property, args)){
      stop(paste("FAIL: property:",
                 paste(deparse(property), collapse = " "),
                 "args:",
                 paste(args, collapse = " ")))}})
  paste ("Pass ", paste(deparse(property), collapse = " "))}

## random data  generators

rdgconstant = function(const) function() const
rdgselect = function(l) function() sample(l,1)[[1]]
rdgmixture = function(...) function() sample(list(...),1)[[1]]()
rdgdistribution = function(distribution, ...) function() distribution(1, ...)

rdgnumericlist = function(lambda) function() lapply(1:rpois(1,lambda), function(i) runif(1))

rdgkeyval = function() function() keyval(runif(1), runif(1))
rdgnumerickeyvallist = function(lambda) function() lapply(1:rpois(1,lambda), function(i) keyval(runif(2)))

## test of the generators

unittest(function(x) rdgconstant(x)() == x, generators = list(rdgdistribution(runif)))
unittest(function(l) is.element(rdgselect(l)(), l), generators = list(rdgnumericlist(10)))
unittest(function(n) is.element(rdgmixture(rdgconstant(n), rdgconstant(2*n))(), list(n,2*n)), 
     generators = list(rdgdistribution(runif)))
unittest(function(d) {
  rdgd = rdgdistribution(d)
  ks.test(d(10000), sapply(1:10000, function(i) rdgd()))$p > 0.001},
     generators = list(rdgselect(list(runif, rnorm))))

## for short
catch.out = function(...) capture.output(invisible(...))
## actual tests
library(RevoHStream)
##createReader
##pending input redirect issue
## send
unittest(function(kv) RevoHStream:::defaulttextoutputformat(kv$key, kv$val) == 
                      paste(catch.out(RevoHStream:::send(kv)),"\n", sep = ""),
                  generators = list(rdgkeyval()))
unittest(function(lkv) {
  all(catch.out(lapply(lkv,RevoHStream:::send)) ==
  catch.out(RevoHStream:::send(lkv)))
},
        generators = list(rdgnumerickeyvallist(10)))
##counter and status -- unused for now
##getKeys and getValues
unittest(function(kvl) all.equal(kvl, 
                             apply(cbind(getKeys(kvl), 
                                         getValues(kvl)),1,keyval)), 
         generators = list(rdgnumerickeyvallist(10)))
##keyval