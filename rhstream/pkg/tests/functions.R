library(RevoHStream)

##main function, should be factored out one day

test = function(property, iterations=10, condition = function(...) T, generators = list()) {
  lapply(1:100, function(i) {
    args = lapply(generators, function(a) do.call(a, list()))
    if(condition(args) && !do.call(property, args)) stop(deparse(list(property, args)))})
  paste ("Pass ", paste(deparse(property), collapse = " "))}

## random data  generators

constant = function(const) function() const
rselect = function(l) function() sample(l,1)
rnumber = function(distribution, ...) function() distr(1,...)

rnumericlist = function(lambda) function() lapply(1:rpois(1,lambda), runif(1))

rnumerickeyvallist = function(lambda) function() lapply(1:rpois(1,lambda), function(i) keyval(runif(2)))


## actual tests
test(function(kvl) all.equal(kvl, 
                             apply(cbind(getKeys(kvl), 
                                         getValues(kvl)),1,keyval)), 
     iterations = 10, 
     generators = list(rnumerickeyvallist(10)))
