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
unit.test = function(predicate, generators, sample.size = 10, precondition = function(...) T) {
  set.seed(0)
  options(warning.length = 8125) #as big as allowed
  results = sapply(1:sample.size, function(i) {
    args = lapply(generators, function(a) a())
    if(do.call(precondition, args) && !do.call(predicate, args)){
      print(paste("FAIL: predicate:",
                 paste(deparse(predicate), collapse = " ")))
      list(predicate = predicate, args = args)
      }}, simplify = TRUE)
  if(length(results) == 0)
    print(paste ("Pass ", paste(deparse(predicate), "\n", collapse = " ")))
  else results}

## test data  generators generators, some generic and some specific to the task at hand
## basic types
tdgg.logical = function(ptrue = .5) function() rbinom(1,1,ptrue) == 1
tdgg.integer = function(lambda = 100) {tdg = tdgg.distribution(rpois, lambda); function() as.integer(tdg())} #why poisson? Why not? Why 100?
tdgg.double = function(min = -1, max = 1)  tdgg.distribution(runif, min, max)
##tdgg.complex NAY
library(digest)
tdgg.character = function(len = 8) function() substr(
  paste(
    sapply(runif(ceiling(len/32)), digest), 
    collapse = ""), 
  1, len)
tdgg.raw = function(len = 8) {tdg = tdgg.character(len); function() charToRaw(tdg())}
tdgg.list = function(tdg = tdgg.any(listtdg = tdg, lambdalist = lambda, maxlevel = maxlevel), 
                    lambda = 10, maxlevel = 20) function() {if(sys.nframe() < maxlevel) replicate(rpois(1, lambda),tdg(), simplify = FALSE) else list()}
tdgg.vector = function(tdg, lambda) {ltdg = tdgg.list(tdg, lambda); function() unlist(ltdg())}
tdgg.data.frame = function(row.lambda = 20, col.lambda = 5){function() {ncol = 1 + rpois(1, col.lambda)
                                                                       nrow = 1 + rpois(1, row.lambda)
                                                                       gens = list(tdgg.logical(), 
                                                                             tdgg.integer(), 
                                                                             tdgg.double(), 
                                                                             tdgg.character())
                                                                       columns = lapply(sample(gens,ncol, replace=TRUE), 
                                                                                        function(g) replicate(nrow, g(), simplify = TRUE))
                                                                       names(columns) = paste("col", 1:ncol)
                                                                       do.call(data.frame, columns)}}

## special distributions
tdgg.fixedlist = function(...) function() lapply(list(...), function(tdg) tdg())
tdgg.prototype = function(prototype) function() rapply(prototype, function(tdg) tdg(), how = "list")
tdgg.prototypelist = function(prototype, lambda) {tdg = tdgg.prototype(prototype); function() replicate(rpois(1, lambda), tdg(), simplify = FALSE)}
tdgg.constant = function(const) function() const
tdgg.select = function(l) function() sample(l,1)[[1]]
tdgg.distribution = function(distribution, ...) function() distribution(1, ...)

##combiners
tdgg.mixture = function(...) function() sample(list(...),1)[[1]]()

## combine everything
tdgg.any = function(ptrue = .5, lambdaint = 100, min = -1, max = 1, lenchar = 8, lenraw = 8, lambdalist = 10, 
                   listtdg = tdgg.any(), lambdavector = 10, maxlevel = 20, vectortdg = tdgg.double()) 
  tdgg.mixture(tdgg.logical(ptrue), 
              tdgg.integer(lambdaint), 
              tdgg.double(min, max), 
              tdgg.character(lenchar), 
  #           tdgg.raw(lenraw),
              tdgg.vector(vectortdg, lambdavector),
              tdgg.list(listtdg, lambdalist, maxlevel))

## generator test thyself
##tdgg.logical 
unit.test(function(ptrue) {
  binom.test(
    sum(replicate(1000,expr = tdgg.logical(ptrue)())),1000, ptrue,"two.sided")$p.value > 0.001},
         generators = list(tdgg.distribution(runif, min = .1, max = .9)))
##tdgg.integer same as tdgg.distribution
##tdgg.double same as tdgg.distribution
##tdgg.complex NAY
##tdgg.character: test legnth, but is it uniform?
unit.test(function(l) nchar(tdgg.character(l)()) == l,
         generators = list(tdgg.integer()))
    
#tdgconstant
unit.test(function(x) tdgg.constant(x)() == x, generators = list(tdgg.distribution(runif)))
#tdgselect
unit.test(function(l) is.element(tdgg.select(l)(), l), generators = list(tdgg.numericlist(10)))
#tdgmixture
unit.test(function(n) is.element(tdgg.mixture(tdgg.constant(n), tdgg.constant(2*n))(), list(n,2*n)), 
     generators = list(tdgg.distribution(runif)))
#tdgdistribution
unit.test(function(d) {
  tdgd = tdgg.distribution(d)
  ks.test(d(10000), sapply(1:10000, function(i) tdgd()))$p > 0.001},
     generators = list(tdgg.select(list(runif, rnorm))))

## for short
catch.out = function(...) capture.output(invisible(...))
## actual tests

