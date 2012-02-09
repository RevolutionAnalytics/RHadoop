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
unittest = function(predicate, generators, samplesize = 10, precondition = function(...) T) {
  set.seed(0)
  options(warning.length = 8125) #as big as allowed
  results = sapply(1:samplesize, function(i) {
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
tdggdata.frame = function(row.lambda = 20, col.lambda = 5){function() {ncol = 1 + rpois(1, col.lambda)
                                                                       nrow = 1 + rpois(1, row.lambda)
                                                                       gens = list(tdgglogical(), 
                                                                             tdgginteger(), 
                                                                             tdggdouble(), 
                                                                             tdggcharacter())
                                                                       columns = lapply(sample(gens,ncol, replace=TRUE), 
                                                                                        function(g) replicate(nrow, g(), simplify = TRUE))
                                                                       names(columns) = paste("col", 1:ncol)
                                                                       do.call(data.frame, columns)}}

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

for (be in c("local", "hadoop")) {
  rmr.options.set(backend = be)
  
  ## test for default writer TBA
  ##counter and status -- unused for now
  
  ## test for typed bytes read/write
  ## replace with unittest when possible
  
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
  unittest(function(kvl) isTRUE(all.equal(kvl, 
                               apply(cbind(keys(kvl), 
                                           values(kvl)),1,function(x) keyval(x[[1]], x[[2]])))), 
           generators = list(tdggkeyvallist()))
  ##keyval
  unittest(function(kv) {
    isTRUE(all.equal(kv,keyval(kv$key,kv$val))) &&      
    attr(kv, "rmr.keyval")},
    generators = list(tdggkeyval()))
  
  ##from.dfs to.dfs
  unittest(function(kvl) {
    isTRUE(all.equal(kvl, from.dfs(to.dfs(kvl)), tolerance = 1e-4, check.attributes = FALSE))},
    generators = list(tdggkeyvallist()),
    samplesize = 10)
    
  ##mapreduce
  
  ##unordered compare
  
  kvl.cmp = function(l1, l2) {
    l1 = l1[order(unlist(keys(l1)))]  
    l2 = l2[order(unlist(keys(l2)))]
    isTRUE(all.equal(l1, l2, tolerance=1e-4, check.attributes=FALSE))}
  
  ##simplest mapreduce, all default
  unittest(function(kvl) {
    if(length(kvl) == 0) TRUE
    else {
      kvl1 = from.dfs(mapreduce(input = to.dfs(kvl)))
      kvl.cmp(kvl, kvl1)}},
           generators = list(tdggkeyvallist(lambda = 10)),
           samplesize = 10)
  
  ##put in a reduce for good measure
  unittest(function(kvl) {
    if(length(kvl) == 0) TRUE
    else {
      kvl1 = from.dfs(mapreduce(input = to.dfs(kvl),
                                reduce = to.reduce.all(identity)))
      kvl.cmp(kvl, kvl1)}},
           generators = list(tdggkeyvallist(lambda = 10)),
           samplesize = 10)

  
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
           unittest(function(kvl) {
             isTRUE(all.equal(kvl, from.dfs(to.dfs(kvl,
                                                   format = fmt),
                                            format = fmt),
                              tolerance = 1e-4,
                              check.attributes = FALSE))},
                    generators = list(tdggkeyvallist()),
                    samplesize = 3))
       
           
  unittest(function(kvl) {
    if(length(kvl) == 0) TRUE
    else {
      kvl1 = from.dfs(
        mapreduce(input = to.dfs(kvl, format = fmt),
                  reduce = to.reduce.all(identity),
                  input.format = fmt,
                  output.format = fmt),
        format = fmt)
      kvl.cmp(kvl, kvl1)}},
           generators = list(tdggkeyvallist(lambda = 10)),
           samplesize = 3)
  }                                           
