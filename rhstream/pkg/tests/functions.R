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

##main function, should be factored out one day

unittest = function(predicate, generators, iterations=10, condition = function(...) T) {
  set.seed(0)
  lapply(1:100, function(i) {
    args = lapply(generators, function(a) a())
    if(do.call(condition, args) && !do.call(predicate, args)){
      stop(paste("FAIL: predicate:",
                 paste(deparse(predicate), collapse = " "),
                 "args:",
                 paste(args, collapse = " ")))}})
  print(paste ("Pass ", paste(deparse(predicate), "\n", collapse = " ")))}

## test data  generators, some generic and some specific to the task at hand
## basic types
tdglogical = function(ptrue =.5) function() rbinom(1,1,ptrue) == 1
tdginteger = function(lambda = 100) tdgdistribution(rpois, lambda) #why poisson? Why not? Why 100?
tdgdouble = function(min = -1, max = 1)  tdgdistribution(runif, min, max)
##tdgcomplex NAY
library(digest)
tdgcharacter = function(len = 8) function() substr(
  paste(
    sapply(runif(ceiling(len/32)), digest), 
    collapse=""), 
  1, len)
tdgraw = function(len = 8) function() charToRaw(tdgcharacter(len)())
tdgfixedlist = function(...) function() lapply(list(...), function(tdg) tdg())
tdgmonomorphiclist = function(tdg, len) function() replicate(len(),tdg()) 

## special distributions
tdgconstant = function(const) function() const
tdgselect = function(l) function() sample(l,1)[[1]]
tdgdistribution = function(distribution, ...) function() distribution(1, ...)

##combiners
tdgmixture = function(...) function() sample(list(...),1)[[1]]()

##app-specific generators
tdgnumericlist = function(lambda) function() lapply(1:rpois(1,lambda), function(i) runif(1))
tdgkeyval = function() function() keyval(runif(1), runif(1)) #we can do better than this
tdgkeyvallist = function(lambda) function() lapply(1:rpois(1,lambda), function(i) keyval(runif(2))) #this one needs work too
tdgnestedkeyvallist = function(lambda) function() lapply(1:rpois(1,lambda), function(i) keyval(runif(1), list(list(runif(2), "a", list("b", 1)))))

## generator test thyself
##tdglogical
unittest(function(ptrue) {
  binom.test(
    sum(replicate(1000,expr = tdglogical(ptrue)())),1000, ptrue,"two.sided")$p.value > 0.001},
         generators = list(tdgdistribution(runif, min = .1, max = .9)))
##tdginteger same as tdgdistribution
##tdgdouble same as tdgdistribution
##tdgcomplex NAY
##tdgcharacter: test legnth, but is it uniform?
unittest(function(l) nchar(tdgcharacter(l)())==l,
         generators = list(tdginteger()))
    
#tdgconstant
unittest(function(x) tdgconstant(x)() == x, generators = list(tdgdistribution(runif)))
#tdgselect
unittest(function(l) is.element(tdgselect(l)(), l), generators = list(tdgnumericlist(10)))
#tdgmixture
unittest(function(n) is.element(tdgmixture(tdgconstant(n), tdgconstant(2*n))(), list(n,2*n)), 
     generators = list(tdgdistribution(runif)))
#tdgdistribution
unittest(function(d) {
  tdgd = tdgdistribution(d)
  ks.test(d(10000), sapply(1:10000, function(i) tdgd()))$p > 0.001},
     generators = list(tdgselect(list(runif, rnorm))))

## for short
catch.out = function(...) capture.output(invisible(...))
## actual tests


library(RevoHStream)
##createReader
##pending input redirect issue
## send
unittest(function(kv) RevoHStream:::defaulttextoutputformat(kv$key, kv$val) == 
                      paste(catch.out(RevoHStream:::send(kv)),"\n", sep = ""),
                  generators = list(tdgkeyval()))
unittest(function(lkv) {
  all(catch.out(lapply(lkv,RevoHStream:::send)) ==
  catch.out(RevoHStream:::send(lkv)))
},
        generators = list(tdgkeyvallist(10)))
##counter and status -- unused for now
##getKeys and getValues
unittest(function(kvl) isTRUE(all.equal(kvl, 
                             apply(cbind(getKeys(kvl), 
                                         getValues(kvl)),1,keyval))), 
         generators = list(tdgkeyvallist(10)))
##keyval
unittest(function(kv) {
  isTRUE(all.equal(kv,keyval(kv$key,kv$val))) &&      
  attr(kv, "keyval")},
  generators = list(tdgkeyval()))

##rhread rhwrite
rhreadwritetest = function(generator) {
  unittest(function(kvl) {
    isTRUE(all.equal(kvl, rhread(rhwrite(kvl)), tolerance=1e-4))},
    generators = list(generator),
    iterations = 10)}

rhreadwritetest(tdgkeyvallist(10))
rhreadwritetest(tdgnestedkeyvallist(10))



                                         