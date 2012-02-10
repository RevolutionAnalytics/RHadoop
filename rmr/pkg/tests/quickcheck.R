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

##app-specific generators
tdgg.numericlist = function(lambda = 100) function() lapply(1:rpois(1,lambda), function(i) runif(1))
tdgg.keyval = function(keytdg = tdgg.double(), valtdg = tdgg.any()) function() keyval(keytdg(), valtdg())
tdgg.keyvalsimple = function() function() keyval(runif(1), runif(1)) #we can do better than this
tdgg.keyvallist = function(keytdg = tdgg.double(), valtdg = tdgg.list(), lambda = 100) tdgg.list(tdg = tdgg.keyval(keytdg, valtdg), lambda = lambda)


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

library(rmr)

for (be in c("local", "hadoop")) {
  rmr.options.set(backend = be)
  
  ## test for default writer TBA
  ##counter and status -- unused for now
  
  ## test for typed bytes read/write
  ## replace with unit.test when possible
  
  lapply(
    list(c(as.raw(66), as.raw(67)), #0
         as.raw(2), #1
         FALSE, #2
         10L, #4
         3.124, #6
         "foobar", #7 
         list(1,"ab", 1L), #8 
         list(keyval(1,"1"), keyval(2, "2"))), #10
    function(x) {
      fname = "/tmp/tbtest"
      con = file(fname, 'wb')
      rmr:::typed.bytes.writer(value=x, con=con)
      close(con)
      con = file(fname, 'rb')
      y = rmr:::typed.bytes.reader(con=con)[[1]]
      close(con)
      stopifnot( all.equal(x,y))})
    
  ##keys and values
  unit.test(function(kvl) isTRUE(all.equal(kvl, 
                               apply(cbind(keys(kvl), 
                                           values(kvl)),1,function(x) keyval(x[[1]], x[[2]])))), 
           generators = list(tdgg.keyvallist()))
  ##keyval
  unit.test(function(kv) {
    isTRUE(all.equal(kv,keyval(kv$key,kv$val))) &&      
    attr(kv, "rmr.keyval")},
    generators = list(tdgg.keyval()))
  
  ##from.dfs to.dfs
  unit.test(function(kvl) {
    isTRUE(all.equal(kvl, from.dfs(to.dfs(kvl)), tolerance = 1e-4, check.attributes = FALSE))},
    generators = list(tdgg.keyvallist()),
    sample.size = 10)
    
  ##mapreduce
  
  ##unordered compare
  
  kvl.cmp = function(l1, l2) {
    l1 = l1[order(unlist(keys(l1)))]  
    l2 = l2[order(unlist(keys(l2)))]
    isTRUE(all.equal(l1, l2, tolerance=1e-4, check.attributes=FALSE))}
  
  ##simplest mapreduce, all default
  unit.test(function(kvl) {
    if(length(kvl) == 0) TRUE
    else {
      kvl1 = from.dfs(mapreduce(input = to.dfs(kvl)))
      kvl.cmp(kvl, kvl1)}},
           generators = list(tdgg.keyvallist(lambda = 10)),
           sample.size = 10)
  
  ##put in a reduce for good measure
  unit.test(function(kvl) {
    if(length(kvl) == 0) TRUE
    else {
      kvl1 = from.dfs(mapreduce(input = to.dfs(kvl),
                                reduce = to.reduce.all(identity)))
      kvl.cmp(kvl, kvl1)}},
           generators = list(tdgg.keyvallist(lambda = 10)),
           sample.size = 10)

  
  ## tour de formats

  x = 1:10; 
  lapply(c("text", "json", "native", "native.text", "sequence.typedbytes"),
         function(fmt)
           as.numeric(values(from.dfs(mapreduce(mapreduce(to.dfs(x), output.format=fmt), input.format=fmt)))) == x)
  
  as.numeric(keys(from.dfs(mapreduce(mapreduce(to.dfs(1:10), output.format="csv"), input.format="csv")))) == x
    
  #roundtrip test without mr
  rt.fmt = c("native", "sequence.typedbytes")
  lapply(rt.fmt,
         function(fmt)
           unit.test(function(kvl) {
             isTRUE(all.equal(kvl, from.dfs(to.dfs(kvl,
                                                   format = fmt),
                                            format = fmt),
                              tolerance = 1e-4,
                              check.attributes = FALSE))},
                    generators = list(tdgg.keyvallist()),
                    sample.size = 3))
       
           
  unit.test(function(kvl) {
    if(length(kvl) == 0) TRUE
    else {
      kvl1 = from.dfs(
        mapreduce(input = to.dfs(kvl, format = fmt),
                  reduce = to.reduce.all(identity),
                  input.format = fmt,
                  output.format = fmt),
        format = fmt)
      kvl.cmp(kvl, kvl1)}},
           generators = list(tdgg.keyvallist(lambda = 10)),
           sample.size = 3)
  }                                           
