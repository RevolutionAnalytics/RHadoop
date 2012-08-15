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
    if(do.call(precondition, args) && !do.call(function(...){tryCatch(predicate(...), error = function(e){traceback(); print(e); FALSE})}, args)){
      print(paste("FAIL: predicate:",
                 paste(deparse(predicate), collapse = " ")))
      list(predicate = predicate, args = args)
      }}, simplify = TRUE)
  if(length(results) == 0)
    print(paste ("Pass ", paste(deparse(predicate), "\n", collapse = " ")))
  else results}

## test data  generators generators, some generic and some specific to the task at hand
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

tdgg.raw = function(len = 8) {tdg = tdgg.character(len); function() charToRaw(tdg())}

tdgg.list = function(tdg = tdgg.any(list.tdg = tdg, lambda.list = lambda, max.level = max.level), 
                    lambda = 10, max.level = 20) function() {if(sys.nframe() < max.level) replicate(rpois(1, lambda),tdg(), simplify = FALSE) else list()}

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
tdgg.numeric.list = function(lambda = 100) function() lapply(1:rpois(1,lambda), function(i) runif(1))
tdgg.fixed.list = function(...) function() lapply(list(...), function(tdg) tdg())
tdgg.prototype = function(prototype) function() rapply(prototype, function(tdg) tdg(), how = "list")
tdgg.prototype.list = function(prototype, lambda) {tdg = tdgg.prototype(prototype); function() replicate(rpois(1, lambda), tdg(), simplify = FALSE)}
tdgg.constant = function(const) function() const
tdgg.select = function(l) function() sample(l,1)[[1]]
tdgg.distribution = function(distribution, ...) function() distribution(1, ...)

##combiners
tdgg.mixture = function(...) function() sample(list(...),1)[[1]]()

## combine everything
tdgg.any = function(p.true = .5, lambda.int = 100, min = -1, max = 1, len.char = 8, len.raw = 8, lambda.list = 10, 
                   list.tdg = tdgg.any(), lambda.vector = 10, max.level = 20, vector.tdg = tdgg.double()) 
  tdgg.mixture(tdgg.logical(p.true), 
              tdgg.integer(lambda.int), 
              tdgg.double(min, max), 
              tdgg.character(len.char), 
  #           tdgg.raw(len.raw),
              tdgg.vector(vector.tdg, lambda.vector),
              tdgg.list(list.tdg, lambda.list, max.level))

