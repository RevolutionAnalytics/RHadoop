//Copyright 2012 Revolution Analytics
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

#include "hbase-to-df.h"


SEXP hbase_to_df(SEXP _source, SEXP _dest) {
	int l = 0;
	Rcpp::List source(_source);
	Rcpp::List dest(_dest);
  Rcpp::List key1 = Rcpp::as<Rcpp::List>(source["key"]);
  Rcpp::List val1 = Rcpp::as<Rcpp::List>(source["val"]);
  for(int i = 0; i < key1.size(); i ++) {
  	std::string key = Rcpp::as<std::string>(key1[i]);
    Rcpp::List key2 = Rcpp::as<Rcpp::List>(val1["key"]);
    Rcpp::List val2 = Rcpp::as<Rcpp::List>(val1["val"]);
  	for(int j = 0; j < key2.size(); j++) {
      std::string family = Rcpp::as<std::string>(key2[j]);
      Rcpp::List key3 = Rcpp::as<Rcpp::List>(val2["key"]);
      Rcpp::List val3 = Rcpp::as<Rcpp::List>(val2["val"]);
      for(int k = 0; k < key3.size(); k++) {
        std::string column = Rcpp::as<std::string>(key3[k]);
        std::vector<unsigned char> cell = Rcpp::as<std::vector<unsigned char> >(val3[k]);
//				dest[1][l] = key;
//				dest[2][l] = family;
//				dest[3][l] = column;
//				l++;}
  }}}
  return Rcpp::wrap(_dest);
}
		