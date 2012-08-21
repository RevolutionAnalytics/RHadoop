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

##main function
unit.test = function(predicate, generators = list(), sample.size = 10, precondition = function(...) TRUE, stop = TRUE) {
  set.seed(0)
  options(warning.length = 8125) #as big as allowed
  results = sapply(1:sample.size, function(i) {
    args = lapply(generators, function(a) a())
    if(do.call(precondition, args) && !do.call(function(...){tryCatch(predicate(...), error = function(e){traceback(); print(e); FALSE})}, args)){
      print(paste("FAIL: predicate:",
                 paste(deparse(predicate), collapse = " ")))
      list(predicate = predicate, args = args)
      }}, simplify = TRUE)
  if(is.null(unlist(results)))
    print(paste ("Pass ", paste(deparse(predicate), "\n", collapse = " ")))
  else {if (stop) stop(results) else results}}

## for short
catch.out = function(...) capture.output(invisible(...))
## test data  generators generators, 

## basic types
tdgg.logical = function(p.true = .5, lambda = 8) function() rbinom(1 + rpois(1, lambda),1,p.true) == 1

tdgg.integer = 
  function(elem.lambda = 100, len.lambda = 8) 
    function() as.integer(rpois(1 + rpois(1, len.lambda), elem.lambda)) #why poisson? Why not? Why 100?

tdgg.double = function(min = -1, max = 1, lambda = 8)  function() runif(1 + rpois(1, lambda), min, max)

##tdgg.complex NAY

library(digest)
tdgg.character = 
  function(str.lambda = 8, len.lambda = 8) 
    function() 
      sapply(runif(1 + rpois(1, len.lambda)), function(x) substr(digest(x), 1, rpois(1, str.lambda)))

tdgg.raw = function(lambda = 8) {tdg = tdgg.character(1, lambda); function() unlist(sapply(tdg(), charToRaw))}

tdgg.list = function(tdg = tdgg.any(list.tdg = tdg, lambda.list = lambda, max.level = max.level), 
                    lambda = 10, max.level = 20) 
  function() {
    if(sys.nframe() < max.level) replicate(rpois(1, lambda),tdg(), simplify = FALSE) else list()}

tdgg.data.frame = 
  function(row.lambda = 20, 
           col.lambda = 5){
    function() {
      ncol = 1 + rpois(1, col.lambda)
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
tdgg.numeric.list = function(lambda = 100) function() lapply(1:rpois(1,lambda), function(i) runif(1))
tdgg.fixed.list = function(...) function() lapply(list(...), function(tdg) tdg())
tdgg.prototype = function(prototype, generator = tdgg.any()) function() rapply(prototype, function(x) generator(), how = "list")

tdgg.prototype.list = 
  function(prototype, lambda) {
    tdg = tdgg.prototype(prototype)
    function() replicate(rpois(1, lambda), tdg(), simplify = FALSE)}

tdgg.constant = function(const) function() const
tdgg.select = function(l) function() sample(l,1)[[1]]
tdgg.distribution = function(distribution, ...) function() distribution(1, ...)

##combiners
tdgg.mixture = function(...) function() sample(list(...),1)[[1]]()

## combine everything
tdgg.any = 
  function(p.true = .5, int.lambda = 100, min = -1, max = 1,  
           list.tdg = tdgg.any(), max.level = 20, len.lambda = 10) 
    tdgg.mixture(tdgg.logical(p.true, len.lambda), 
                 tdgg.integer(int.lambda, len.lambda), 
                 tdgg.double(min, max, len.lambda), 
                 tdgg.character(len.lambda), 
                 tdgg.raw(len.lambda),
                 tdgg.list(list.tdg, len.lambda, max.level))

