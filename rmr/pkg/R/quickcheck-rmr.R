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

##app-specific generators
library(quickcheck)

tdgg.keyval = function(keytdg = tdgg.double(), valtdg = tdgg.any()) function() keyval(keytdg(), valtdg())
tdgg.keyvalsimple = function() function() keyval(runif(1), runif(1)) #we can do better than this
tdgg.keyval.list = 
  function(keytdg = tdgg.double(), valtdg = tdgg.list(), lambda = 100) 
    tdgg.list(tdg = tdgg.keyval(keytdg, valtdg), lambda = lambda)
