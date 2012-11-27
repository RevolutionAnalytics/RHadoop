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
typedef std::deque<unsigned char> raw;

std::string raw_to_string(SEXP source) {
  Rcpp::RawVector raw_source(source);
  std::string retval(raw_source.size(), 'a');
  std::copy(raw_source.begin(), raw_source.end(), retval.begin());
  return retval;}

SEXP raw_list_to_character(SEXP _source) {
  Rcpp::List source(_source);
  Rcpp::CharacterVector dest(source.size());
  for(int i = 0; i < source.size(); i++) {
    dest[i] = raw_to_string(source[i]);}
  return Rcpp::wrap(dest);}

SEXP string_to_raw(std::string source) {
  Rcpp::RawVector retval(source.size());
  std::copy(source.begin(), source.end(), retval.begin());
  return Rcpp::wrap(retval);}

SEXP p_string_to_raw(SEXP _source) {
  std::vector<std::string> source = Rcpp::as<std::vector<std::string> >(_source);
  Rcpp::List retval(source.size());
  for(int i = 0; i < source.size(); i++) {
    retval[i] = Rcpp::wrap(string_to_raw(source[i]));}
  return Rcpp::wrap(retval);}
    
SEXP hbase_to_df(SEXP _source, SEXP _dest) {
	int l = 0;
	
  Rcpp::List dest(_dest);
  Rcpp::List dest_key    = Rcpp::as<Rcpp::List>(dest["key"]);
  Rcpp::List dest_family = Rcpp::as<Rcpp::List>(dest["family"]);
  Rcpp::List dest_column = Rcpp::as<Rcpp::List>(dest["column"]);
  Rcpp::List dest_cell   = Rcpp::as<Rcpp::List>(dest["cell"]);
  
  Rcpp::List source(_source);
  Rcpp::List key1 = Rcpp::as<Rcpp::List>(source["key"]);
  Rcpp::List val1 = Rcpp::as<Rcpp::List>(source["val"]);
  
  for(int i = 0; i < key1.size(); i ++) {
    Rcpp::List val1_i = Rcpp::as<Rcpp::List>(val1[i]);
    Rcpp::List key2   = Rcpp::as<Rcpp::List>(val1_i["key"]);
    Rcpp::List val2   = Rcpp::as<Rcpp::List>(val1_i["val"]);
	  for(int j = 0; j < key2.size(); j++) {
      Rcpp::List val2_j = Rcpp::as<Rcpp::List>(val2[j]);
      Rcpp::List key3   = Rcpp::as<Rcpp::List>(val2_j["key"]);
      Rcpp::List val3   = Rcpp::as<Rcpp::List>(val2_j["val"]);
      for(int k = 0; k < key3.size(); k++) {
        dest_family[l] = Rcpp::wrap(key2[j]);
				dest_column[l] = Rcpp::wrap(key3[k]);
        dest_key[l]    = Rcpp::wrap(key1[i]);
  			dest_cell[l]   = Rcpp::wrap(val3[k]);
  			l++;}}}
  return Rcpp::wrap(
    Rcpp::List::create(
      Rcpp::Named("data.frame") = Rcpp::wrap(_dest),
      Rcpp::Named("nrows") = Rcpp::wrap(l)));}
		