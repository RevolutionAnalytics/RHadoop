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

library(quickcheck)
## generator test thyself
##tdgg.logical 
unit.test(function(p.true) {
  sample = tdgg.logical(p.true,lambda=1000)()
  binom.test(
    sum(sample),
    length(sample), 
    p.true,"two.sided")$p.value > 0.001},
          generators = list(tdgg.distribution(runif, min = .1, max = .9)))
##tdgg.integer 
unit.test(is.integer,
          generators = list(tdgg.integer()))
##tdgg.double 
unit.test(is.double,
          generators = list(tdgg.double()))
##tdgg.complex NAY
##tdgg.character: 
unit.test(is.character,
          generators = list(tdgg.character()))

##tdgg.raw
unit.test(is.raw,
          generators = list(tdgg.raw()))

#tdgconstant
unit.test(function(x) tdgg.constant(x)() == x, generators = list(tdgg.distribution(runif)))
#tdgselect
unit.test(function(l) is.element(tdgg.select(l)(), l), generators = list(tdgg.numeric.list(10)))
#tdgmixture
unit.test(function(n) is.element(tdgg.mixture(tdgg.constant(n), tdgg.constant(2*n))(), list(n,2*n)), 
          generators = list(tdgg.distribution(runif)))
#tdgdistribution
unit.test(function(d) {
  tdgd = tdgg.distribution(d)
  ks.test(d(10000), sapply(1:10000, function(i) tdgd()))$p > 0.001},
          generators = list(tdgg.select(list(runif, rnorm))))
# tdgg.list
# tdgg.data.frame 
# tdgg.numeric.list
# tdgg.fixed.list
# tdgg.prototype
# tdgg.prototype.list
# tdgg.constant
# tdgg.select
# tdgg.mixture 
# tdgg.any 