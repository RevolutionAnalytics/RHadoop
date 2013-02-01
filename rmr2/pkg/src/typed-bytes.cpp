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

#include "typed-bytes.h"
#include <deque>
#include <iostream>
#include <algorithm>
#include <math.h>

typedef std::deque<unsigned char> raw;
#include <string>
#include <iostream>

void safe_stop(std::string message) {
  try{
    throw Rcpp::exception(message.c_str(), "typed-bytes.cpp", 27);}//switch to rcpp::stop when available for revoR
  catch( std::exception &ex ) {
    forward_exception_to_r( ex );}}
    
template<typename T>
void stop_unimplemented(std::string what){
  safe_stop(what + " unimplemented for " + typeid(T).name());}
    
class ReadPastEnd {
public:
  std::string type_code;
  int start;
  ReadPastEnd(std::string _type_code, int _start){
    type_code = _type_code;
    start = _start;}};

class UnsupportedType{
public:
  unsigned char type_code;
  UnsupportedType(unsigned char _type_code){
    type_code = _type_code;}};

class NegativeLength {
public:
  NegativeLength(){}};
  
template <typename T> 
int nbytes(){
  stop_unimplemented<T>("nbytes");}

//nbytes provides the size of a typedbytes type corresponding to a C type
template<>
int nbytes<int>(){return 4;}

template<>
int nbytes<long>(){return 8;}

template<>
int nbytes<double>(){return 8;}

template<>
int nbytes<bool>(){return 1;}

template<>
int nbytes<unsigned char>(){return 1;}

template<>
int nbytes<char>(){return 1;}

template <typename T>
void check_length(const raw & data, int start, int length = nbytes<T>()) {
  if(data.size() < start + length) {
      throw ReadPastEnd(typeid(T).name(), start);}}

template <typename T> 
T unserialize_integer(const raw & data, int & start) {
  check_length<T>(data, start);
  int retval = 0;
  for (int i = 0; i < nbytes<T>(); i ++) {
    retval = retval + ((data[start + i] & 255) << (8*((nbytes<T>()-1) - i)));}
  start = start + nbytes<T>();
  return retval;}

template <typename T>
T unserialize_numeric(const raw & data, int & start){
  stop_unimplemented<T>("unserialize_numeric called");}

template<>
double unserialize_numeric<double>(const raw & data, int & start) {
  union udouble {
    double d;
    uint64_t u;} ud;
  check_length<double>(data, start);
  uint64_t retval = 0;
  for(int i = 0; i < nbytes<double>(); i++) {
    retval = retval + (((uint64_t) data[start + i] & 255) << (8*(7 - i)));}
  start = start + nbytes<double>(); 
  ud.u = retval;
  return ud.d;} 
 
template <typename T>
T unserialize_scalar(const raw & data, int & start){
  if(nbytes<T>() > 1) {
    stop_unimplemented<T>("Multibyte unserialize_scalar ");}
  check_length<T>(data, start);
  start = start + nbytes<T>();
  return (T)data[start - nbytes<T>()];}

template<>
int unserialize_scalar<int>(const raw & data, int & start){
  return unserialize_integer<int>(data, start);}
  
template<>
long unserialize_scalar<long>(const raw & data, int & start){
  return unserialize_integer<long>(data, start);}
  
template<>
double unserialize_scalar<double>(const raw & data, int & start){
  return unserialize_numeric<double>(data, start);}
  
template<>
float unserialize_scalar<float>(const raw & data, int & start){
  return unserialize_numeric<float>(data, start);}
    
int get_length(const raw & data, int & start) {
  int len = unserialize_scalar<int>(data, start);
  if(len < 0) {
  	throw NegativeLength();}
  return len;}
  
int get_type(const raw & data, int & start) {
  int debug = (int)unserialize_scalar<unsigned char>(data, start); 
  return debug;}
  
template <typename T>
std::vector<T> unserialize_vector(const raw & data, int & start, int raw_length) {
  int length = raw_length/nbytes<T>();
  std::vector<T> vec(length);
  for(int i = 0; i < length; i++) {
    vec[i] = unserialize_scalar<T>(data, start);}
  return vec;}
  
template <>
std::vector<std::string> unserialize_vector<std::string>(const raw & data, int & start, int raw_length) {
  int v_length = get_length(data, start);
  std::vector<std::string> retval(v_length);
  for(int i = 0; i < v_length; i++) {
    get_type(data, start); //we know it's 07 already
    int str_length = get_length(data, start);
    std::vector<char> tmp_vec_char = unserialize_vector<char>(data, start, str_length);
    std::string tmp_string(tmp_vec_char.begin(), tmp_vec_char.end());
    retval[i] = tmp_string;}
  return retval;}
      
template <>
SEXP unserialize_scalar<SEXP>(const raw & data, int & start) {
  int length = get_length(data, start);
  check_length<SEXP>(data, start, length);
  Rcpp::Function r_unserialize("unserialize");
  raw tmp(data.begin() + start, data.begin() + start + length);
  start = start + length;
  return r_unserialize(Rcpp::wrap(tmp));}

template <typename T>
SEXP wrap_unserialize_vector(const raw & data, int & start, int length) {
  return Rcpp::wrap(unserialize_vector<T>(data, start, length));}

template <typename T>
SEXP wrap_unserialize_scalar(const raw & data, int & start) {
  return Rcpp::wrap(unserialize_scalar<T>(data, start));}

SEXP unserialize(const raw & data, int & start, int type_code = 255){
  Rcpp::RObject new_object;
  if(type_code == 255) {
    type_code = get_type(data, start);}
  switch(type_code) {
    case 0: { //byte vector
      int length = get_length(data, start);
      new_object = wrap_unserialize_vector<unsigned char>(data, start, length);}
      break;
    case 1: { //byte
      new_object = wrap_unserialize_scalar<unsigned char>(data, start);}
      break;
    case 2: { //boolean
      new_object = wrap_unserialize_scalar<bool>(data, start);}
      break;
    case 3: { //integer
      new_object = wrap_unserialize_scalar<int>(data, start);}      
      break;
    case 4: { //long
      new_object = wrap_unserialize_scalar<long>(data, start);} 
      break;
    case 5: { //float
      throw UnsupportedType(type_code);}
      break;
    case 6: { //double
      new_object = wrap_unserialize_scalar<double>(data, start);}
      break;
    case 7: { //string
      int length = get_length(data, start);
      std::vector<char> vec_tmp = unserialize_vector<char>(data, start, length);
      new_object =  Rcpp::wrap(std::string(vec_tmp.begin(), vec_tmp.end()));}
      break;
    case 8: { //vector
      int length = get_length(data, start);
      Rcpp::List list(length);
      int list_end = 0; 
      for(int i = 0; i < length; i++) {
        list[i] = unserialize(data, start);}
      new_object = Rcpp::wrap(list);}
      break;
    case 9: // list (255 terminated vector)
    case 10: { //map
      throw UnsupportedType(type_code);}
      break;
    case 144: { //R serialization
      new_object = unserialize_scalar<SEXP>(data, start);}
      break;
    case 145: {
      int raw_length = get_length(data, start);
      int vec_type_code  = get_type(data, start);
      raw_length = raw_length - 1;
      switch(vec_type_code) {
        case 1:{
          new_object = wrap_unserialize_vector<unsigned char>(data, start, raw_length);}
        break;
        case 2:{
          new_object = wrap_unserialize_vector<bool>(data, start, raw_length);}
        break;
        case 3:{
          new_object = wrap_unserialize_vector<int>(data, start, raw_length);}
        break;
        case 4:{
          new_object = wrap_unserialize_vector<long>(data, start, raw_length);}
        break;
        case 5:{
          new_object = wrap_unserialize_vector<float>(data, start, raw_length);}
        break;
        case 6:{
          new_object = wrap_unserialize_vector<double>(data, start, raw_length);}
        break;
        default: {
          throw UnsupportedType(vec_type_code);}}}
      break;
    case 146: {
       int raw_length = get_length(data, start);
       new_object = wrap_unserialize_vector<std::string>(data, start, raw_length);}
      break;
    default: {
      throw UnsupportedType(type_code);}}
      return new_object;}

SEXP typed_bytes_reader(SEXP data, SEXP _nobjs){
	Rcpp::NumericVector nobjs(_nobjs);
	Rcpp::List objs(floor(nobjs[0]));
	Rcpp::RawVector tmp(data);
	raw rd(tmp.begin(), tmp.end());
	int start = 0;
	int parsed_start = 0;
	int objs_end = 0;
	while(rd.size() > start && objs_end < nobjs[0]) {
 		try{
      objs[objs_end] = unserialize(rd, start);
      objs_end = objs_end + 1;
      parsed_start = start;}
    catch (ReadPastEnd rpe){
      break;}
		catch (UnsupportedType ue) {
      safe_stop("Unsupported type: " + (int)ue.type_code);}
		catch (NegativeLength nl) {
      safe_stop("Negative length exception");}}
  Rcpp::List list_tmp(objs.begin(), objs.begin() + objs_end);
	return Rcpp::wrap(
    Rcpp::List::create(
      Rcpp::Named("objects") = Rcpp::wrap(list_tmp),
      Rcpp::Named("length") = Rcpp::wrap(parsed_start)));}

void T2raw(unsigned char data, raw & serialized) {
  serialized.push_back(data);}

void T2raw(int data, raw & serialized) {
  for(int i = 0; i < 4; i++) {
    serialized.push_back((data >> (8*(3 - i))) & 255);}}

void T2raw(uint64_t data, raw & serialized) {
  for(int i = 0; i < 8; i++) {  
    serialized.push_back((data >> (8*(7 - i))) & 255);}}

void T2raw(double data, raw & serialized) {
  union udouble {
    double d;
    uint64_t u;} ud;
  ud.d = data;
  T2raw(ud.u, serialized);}


void length_header(int len, raw & serialized){
  if(len < 0) {
  	throw NegativeLength();}
  T2raw(len, serialized);}

template <typename T>
void serialize_scalar(const T & data, unsigned char type_code, raw & serialized) {
  if(type_code != 255) serialized.push_back(type_code);
  T2raw(data, serialized);}

template <typename T> 
void serialize_many(const T & data, unsigned char type_code, raw & serialized){
  serialized.push_back(type_code);
  length_header(data.size(), serialized);
  serialized.insert(serialized.end(), data.begin(), data.end());}

void serialize(const SEXP & object, raw & serialized, bool native); 

template <typename T> 
void serialize_vector(T & data, unsigned char type_code, raw & serialized, bool native){  
  if(data.size() == 1) {
    serialize_scalar(data[0], type_code, serialized);}
  else {
    if(native) {
      serialized.push_back(145);
      length_header(data.size() * sizeof(data[0]) + 1, serialized); 
      serialized.push_back(type_code);
      for(typename T::iterator i = data.begin(); i < data.end(); i++) {
        serialize_scalar(*i, 255, serialized);}}
    else {
      serialized.push_back(8);
      length_header(data.size(), serialized); 
      for(typename T::iterator i = data.begin(); i < data.end(); i++) {
        serialize_scalar(*i, type_code, serialized);}}}}

template <typename T> void serialize_list(T & data, raw & serialized){
  serialized.push_back(8);
  length_header(data.size(), serialized);
  for(typename T::iterator i = data.begin(); i < data.end(); i++) {
    serialize(Rcpp::wrap(*i), serialized, FALSE);}}

void serialize_native(const SEXP & object, raw & serialized) {
  serialized.push_back(144);
  Rcpp::Function r_serialize("serialize");
  Rcpp::RawVector tmp(r_serialize(object, R_NilValue));
  length_header(tmp.size(), serialized);
  serialized.insert(serialized.end(), tmp.begin(), tmp.end());}

void serialize(const SEXP & object, raw & serialized, bool native) {
  Rcpp::RObject robj(object);
  bool has_attr = robj.attributeNames().size() > 0;
  if(native) {
    switch(robj.sexp_type()) {
      case LGLSXP: {
        if(has_attr) {
          serialize_native(object, serialized);}
        else {
          Rcpp::LogicalVector data(object);  
          std::vector<unsigned char> bool_data(data.size());
          for(int i = 0; i < data.size(); i++) {
            bool_data[i] = (unsigned char) data[i];} 
          serialize_vector(bool_data, 2, serialized, TRUE);}}
      break;
      case REALSXP: {
        if(has_attr) {
          serialize_native(object, serialized);}
        else {
          Rcpp::NumericVector data(object);  
          serialize_vector(data, 6, serialized, TRUE);}}
      break;
      case STRSXP: { //character
        Rcpp::CharacterVector data(object);
        serialized.push_back(146);
        int raw_size = data.size() * 5 + 4;
        for(int i = 0; i < data.size(); i++) {
          raw_size += data[i].size();}
        length_header(raw_size, serialized);
        length_header(data.size(), serialized);
        for(int i = 0; i < data.size(); i++) {
          serialize_many(data[i], 7, serialized);}}
      break; 
      case INTSXP: {
        if(has_attr) {
          serialize_native(object, serialized);}
        else {
          Rcpp::IntegerVector data(object);  
          serialize_vector(data, 3, serialized, TRUE);}}
      break;
      default:
        serialize_native(object, serialized);}}
  else {
    switch(robj.sexp_type()) {
    	case NILSXP: {
      	  throw UnsupportedType(NILSXP);}
    	  break;
      case RAWSXP: {//raw
        Rcpp::RawVector data(object);
        serialize_many(data, 0, serialized);}
        break;
      case STRSXP: { //character
        Rcpp::CharacterVector data(object);
        if(data.size() > 1) {
          serialized.push_back(8);
          length_header(data.size(), serialized);}
        for(int i = 0; i < data.size(); i++) {
          serialize_many(data[i], 7, serialized);}}
        break; 
      case LGLSXP: { //logical
        Rcpp::LogicalVector data(object);
        std::vector<unsigned char> bool_data(data.size());
        for(int i = 0; i < data.size(); i++) {
          bool_data[i] = (unsigned char) data[i];}
        serialize_vector(bool_data, 2, serialized, FALSE);}
        break;
      case REALSXP: { //numeric
        Rcpp::NumericVector data(object);
        serialize_vector(data, 6, serialized, FALSE);}
        break;
      case INTSXP: { //factor, integer
        Rcpp::IntegerVector data(object);
        serialize_vector(data, 3, serialized, FALSE);}
        break;
      case VECSXP: { //list
        Rcpp::List data(object);
        serialize_list(data, serialized);}
        break;
      default: {
        throw UnsupportedType(robj.sexp_type());}}}}

SEXP typed_bytes_writer(SEXP objs, SEXP native){
	raw serialized(0);
	Rcpp::List objects(objs);
  Rcpp::LogicalVector is_native(native);
	for(Rcpp::List::iterator i = objects.begin(); i < objects.end(); i++) {
    try{
      serialize(Rcpp::wrap(*i), serialized, is_native[0]);}
    catch(UnsupportedType ut){
      safe_stop("Unsupported type: " + (int)ut.type_code);}}
	return Rcpp::wrap(serialized);}	


