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

unittest = function(predicate, generators, iterations = 10, condition = function(...) T) {
  set.seed(0)
  options(warning.length = 8125)
  lapply(1:100, function(i) {
    args = lapply(generators, function(a) a())
    if(do.call(condition, args) && !do.call(predicate, args)){
      stop(paste("FAIL: predicate:",
                 paste(deparse(predicate), collapse = " "),
                 "args:",
                 paste(args, collapse = " ")))}})
  print(paste ("Pass ", paste(deparse(predicate), "\n", collapse = " ")))}

## test data  generators generators, some generic and some specific to the task at hand
## basic types
tdgglogical = function(ptrue = .5) function() rbinom(1,1,ptrue) == 1
tdgginteger = function(lambda = 100) {tdg = tdggdistribution(rpois, lambda); function() as.integer(tdg())} #why poisson? Why not? Why 100?
tdggdouble = function(min = -1, max = 1)  tdggdistribution(runif, min, max)
##tdggcomplex NAY
library(digest)
tdggcharacter = function(len = 8) function() substr(
  paste(
    sapply(runif(ceiling(len/32)), digest), 
    collapse = ""), 
  1, len)
tdggraw = function(len = 8) {tdg = tdggcharacter(len); function() charToRaw(tdg())}
tdgglist = function(tdg = tdggany(listtdg = tdg, lambdalist = lambda, maxlevel = maxlevel), 
                    lambda = 10, maxlevel = 20) function() {if(sys.nframe() < maxlevel) replicate(rpois(1, lambda),tdg(), simplify = FALSE) else list()}
tdggvector = function(tdg, lambda) {ltdg = tdgglist(tdg, lambda); function() unlist(ltdg())}

## special distributions
tdggfixedlist = function(...) function() lapply(list(...), function(tdg) tdg())
tdggprototype = function(prototype) function() rapply(prototype, function(tdg) tdg(), how = "list")
tdggprototypelist = function(prototype, lambda) {tdg = tdggprototype(prototype); function() replicate(rpois(1, lambda), tdg(), simplify = FALSE)}
tdggconstant = function(const) function() const
tdggselect = function(l) function() sample(l,1)[[1]]
tdggdistribution = function(distribution, ...) function() distribution(1, ...)

##combiners
tdggmixture = function(...) function() sample(list(...),1)[[1]]()

## combine everything
tdggany = function(ptrue = .5, lambdaint = 100, min = -1, max = 1, lenchar = 8, lenraw = 8, lambdalist = 10, 
                   listtdg = tdggany(), lambdavector = 10, maxlevel = 20, vectortdg = tdggdouble()) 
  tdggmixture(tdgglogical(ptrue), 
              tdgginteger(lambdaint), 
              tdggdouble(min, max), 
              tdggcharacter(lenchar), 
  #           tdggraw(lenraw),
              tdggvector(vectortdg, lambdavector),
              tdgglist(listtdg, lambdalist, maxlevel))

##app-specific generators
tdggnumericlist = function(lambda = 100) function() lapply(1:rpois(1,lambda), function(i) runif(1))
tdggkeyval = function(keytdg = tdggdouble(), valtdg = tdggany()) function() keyval(keytdg(), valtdg())
tdggkeyvalsimple = function() function() keyval(runif(1), runif(1)) #we can do better than this
tdggkeyvallist = function(keytdg = tdggdouble(), valtdg = tdgglist(), lambda = 100) tdgglist(tdg = tdggkeyval(keytdg, valtdg), lambda = lambda)


## generator test thyself
##tdgglogical 
unittest(function(ptrue) {
  binom.test(
    sum(replicate(1000,expr = tdgglogical(ptrue)())),1000, ptrue,"two.sided")$p.value > 0.001},
         generators = list(tdggdistribution(runif, min = .1, max = .9)))
##tdgginteger same as tdggdistribution
##tdggdouble same as tdggdistribution
##tdggcomplex NAY
##tdggcharacter: test legnth, but is it uniform?
unittest(function(l) nchar(tdggcharacter(l)()) == l,
         generators = list(tdgginteger()))
    
#tdgconstant
unittest(function(x) tdggconstant(x)() == x, generators = list(tdggdistribution(runif)))
#tdgselect
unittest(function(l) is.element(tdggselect(l)(), l), generators = list(tdggnumericlist(10)))
#tdgmixture
unittest(function(n) is.element(tdggmixture(tdggconstant(n), tdggconstant(2*n))(), list(n,2*n)), 
     generators = list(tdggdistribution(runif)))
#tdgdistribution
unittest(function(d) {
  tdgd = tdggdistribution(d)
  ks.test(d(10000), sapply(1:10000, function(i) tdgd()))$p > 0.001},
     generators = list(tdggselect(list(runif, rnorm))))

## for short
catch.out = function(...) capture.output(invisible(...))
## actual tests


library(rmr)
##createReader
##pending input redirect issue: use textConnection
## send
unittest(function(kv) rmr:::defaulttextoutputformat(kv$key, kv$val) == 
                      paste(catch.out(rmr:::send(kv)),"\n", sep = ""),
                  generators = list(tdggkeyval()))
unittest(function(lkv) {
  all(catch.out(lapply(lkv,rmr:::send)) == 
  catch.out(rmr:::send(lkv)))
},
        generators = list(tdggkeyvallist()))
##counter and status -- unused for now
##keys and values
unittest(function(kvl) isTRUE(all.equal(kvl, 
                             apply(cbind(keys(kvl), 
                                         values(kvl)),1,keyval))), 
         generators = list(tdggkeyvallist()))
##keyval
unittest(function(kv) {
  isTRUE(all.equal(kv,keyval(kv$key,kv$val))) &&      
  attr(kv, "keyval")},
  generators = list(tdggkeyval()))

##from.dfs to.dfs
from.to.dfs.test = function(generator) {
  unittest(function(kvl) {
    isTRUE(all.equal(kvl, from.dfs(to.dfs(kvl)), tolerance = 1e-4, check.attributes = FALSE))},
    generators = list(generator),
    iterations = 10)}

from.to.dfs.test(tdggkeyvallist())

##mapreduce

unittest(function(kvl) {
  if(length(kvl) == 0) TRUE
  else {
    kvl = kvl[order(unlist(keys(kvl)))]
    kvl1 = from.dfs(mapreduce(input = to.dfs(kvl)))
    kvl1 = kvl1[order(unlist(keys(kvl1)))]
    isTRUE(all.equal(kvl, kvl1, tolerance = 1e-4, check.attributes = FALSE))}},
  generators = list(tdggkeyvallist(lambda = 10)),
  iterations = 10)

                                         
