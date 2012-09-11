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

#include "dataframe-to-list.h"

class UnsupportedType{
public:
  unsigned char type_code;
  UnsupportedType(unsigned char _type_code){
    type_code = _type_code;};};
    
SEXP dataframe_to_list(SEXP _source, SEXP _nrow, SEXP _ncol, SEXP _dest){
	Rcpp::DataFrame source = Rcpp::DataFrame(_source);
	Rcpp::GenericVector dest = Rcpp::GenericVector(_dest);
	int nrow = Rcpp::as<int>(_nrow);
	int ncol = Rcpp::as<int>(_ncol);	
	for (int j = 0; j < ncol; j ++){
		Rcpp::RObject robj = source[j];
		switch(robj.sexp_type()) {
			case RAWSXP: {
				Rcpp::RawVector z = source[j]; 
				for (int i = 0; i < nrow; i ++) {
					Rcpp::List l = dest[i];
					l[j] = Rcpp::wrap(z[i]);}}
			break;
			case STRSXP: {
				Rcpp::CharacterVector z = source[j]; 
				for (int i = 0; i < nrow; i ++) {
					Rcpp::List l = dest[i];
					l[j] = Rcpp::wrap(z[i]);}}
			break;
			case LGLSXP: {
				Rcpp::LogicalVector z = source[j]; 
				for (int i = 0; i < nrow; i ++) {
					Rcpp::List l = dest[i];
					l[j] = Rcpp::wrap(z[i]);}}
			break;
			case REALSXP: {
				Rcpp::NumericVector z = source[j]; 
				for (int i = 0; i < nrow; i ++) {
					Rcpp::List l = dest[i];
					l[j] = Rcpp::wrap(z[i]);}}
			break;
			case INTSXP: {
				Rcpp::IntegerVector z = source[j]; 
				for (int i = 0; i < nrow; i ++) {
					Rcpp::List l = dest[i];
					l[j] = Rcpp::wrap(z[i]);}}
			break;
			default: {
				throw UnsupportedType(robj.sexp_type());}}}
	return Rcpp::wrap(_dest);}
