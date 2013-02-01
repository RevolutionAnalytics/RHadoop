//Copyright 2011 Revolution Analytics
//   
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS, 
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

#include "extras.h"
#include <vector>
#include <Rcpp.h>

SEXP vsum(SEXP xx) {
  Rcpp::List xx_ (xx);
  std::vector<double> results(xx_.size());
  for(int i = 0; i < xx_.size(); i ++) {
    std::vector<double> x = Rcpp::as<std::vector<double> >(xx_[i]);
    for(int j = 0; j < x.size(); j++) {
      results[i] += x[j];}}
  return Rcpp::wrap(results);}