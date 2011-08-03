library(RevoHStream)

##main function, should be factored out one day

test = function(property, iterations=10, condition = function(...) T, generators = list()) {
  lapply(1:100, function(i) {
    args = lapply(generators, function(a) do.call(a, list()))
    if(do.call(condition, args) && !do.call(property, args)) stop(deparse(list(property, args)))})
  paste ("Pass ", paste(deparse(property), collapse = " "))}

## random data  generators

rdgconstant = function(const) function() const
rdgselect = function(l) function() sample(l,1)
rdgunion = function(...) function() sample(list(...),1)[[1]]()
rdgdistribution = function(distribution, ...) function() distribution(1, ...)

rdgnumericlist = function(lambda) function() lapply(1:rpois(1,lambda), function(i) runif(1))

rdgnumerickeyvallist = function(lambda) function() lapply(1:rpois(1,lambda), function(i) keyval(runif(2)))

## test of the tests

test(function(x) rdgconstant(x)() == x, generators = list(rdgdistribution(runif)))
test(function(l) is.element(rdgselect(l)(), l), generators = list(rdgnumericlist(10)))
test(function(n) is.element(rdgunion(rdgconstant(n), rdgconstant(2*n))(), list(n,2*n)), 
     generators = list(rdgdistribution(runif)))
test(function() {
  rdgd = rdgdistribution(runif)
  ks.test(runif(10000), sapply(1:10000, function(i) rdgd()))$p > 0.001})



## actual tests
test(function(kvl) all.equal(kvl, 
                             apply(cbind(getKeys(kvl), 
                                         getValues(kvl)),1,keyval)), 
     iterations = 10, 
     generators = list(rdgnumerickeyvallist(10)))
