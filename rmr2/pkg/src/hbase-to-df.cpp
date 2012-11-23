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
	  for(int i = 0; i < source[1].size(); i ++) {
		key = source[1][i];
		for(int j = 1; j <= length(source[2][i][1]); j++) {
			family = source[2][i][1][j];
			for(int k = 1; k <= length(source[2][j][1]); k++) {
				column = source[2][i][2][j][1][k]
				cell = source[2][i][2][j][2][k]
				dest[1][l] = key;
				dest[2][l] = family;
				dest[3][l] = column;
				l++;}}}
		