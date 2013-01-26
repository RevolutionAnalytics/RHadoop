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
#include <stdint.h>

typedef std::deque<unsigned char> raw;
#include <string>
#include <iostream>

class ReadPastEnd {
public:
  std::string type_code;
  int start;
  ReadPastEnd(std::string _type_code, int _start){
    type_code = _type_code;
    start = _start;
  };};

class UnsupportedType{
public:
  unsigned char type_code;
  UnsupportedType(unsigned char _type_code){
    type_code = _type_code;};};

class NegativeLength {
public:
  NegativeLength(){};};
  
template <typename T> 
int nbytes(){}

template<>
int nbytes<int>(){return 4;}

template<>
int nbytes<long>(){return 8;}

template <typename T> 
T unserialize_integer(const raw & data, int & start) {
  if(data.size() < start + nbytes<T>()) {
    throw ReadPastEnd(typeid(T).name(), start);}
  int retval = 0;
  for (int i = 0; i < nbytes<T>(); i ++) {
    retval = retval + ((data[start + i] & 255) << (8*((nbytes<T>()-1) - i)));}
  start = start + nbytes<T>();
  return retval;}

template <typename T>
T unserialize_numeric(const raw & data, int & start){}

template<>
double unserialize_numeric<double>(const raw & data, int & start) {
  union udouble {
    double d;
    uint64_t u;} ud;

  if(data.size() < start + 8) {
    throw ReadPastEnd("double", start);  }
  uint64_t retval = 0;
  for(int i = 0; i < 8; i++) {
    retval = retval + (((uint64_t) data[start + i] & 255) << (8*(7 - i)));}
  start = start + 8; 

  ud.u = retval;
  return ud.d;} 
 
template <typename T>
T unserialize_one(const raw & data, int & start){}

template<>
int unserialize_one<int>(const raw & data, int & start){
  return unserialize_integer<int>(data, start);}
  
template<>
long unserialize_one<long>(const raw & data, int & start){
  return unserialize_integer<long>(data, start);}
  
template<>
double unserialize_one<double>(const raw & data, int & start){
  return unserialize_numeric<double>(data, start);}
  
template<>
float unserialize_one<float>(const raw & data, int & start){
  return unserialize_numeric<float>(data, start);}
  
unsigned char unserialize_character(const raw & data, int & start) {
  if(data.size() < start + 1) {
    throw ReadPastEnd("byte", start);}
  start = start + 1;    
  return(unsigned char)(data[start -1]);}

template<>
unsigned char unserialize_one<unsigned char>(const raw & data, int & start){
  return unserialize_character(data, start);}
  
int get_length(const raw & data, int & start) {
  int len = unserialize_one<int>(data, start);
  if(len < 0) {
  	throw NegativeLength();}
  return len;}

unsigned char get_type(const raw & data, int & start) {
  if(data.size() < start + 1) {
    throw ReadPastEnd("type code", start);}
  unsigned char retval  = data[start];
  start = start + 1;
  return retval;}

void unserialize_fixed(const raw & data, int & raw_start, Rcpp::List & objs, int & objs_end, unsigned char vec_type_code, int length) {
    switch(vec_type_code) {
      case 1: { //byte
      if(data.size() < raw_start + length) {
        throw ReadPastEnd("byte", raw_start);}
      std::vector<unsigned char> vec;
      for(int i = 0; i < length; i++) {
        vec.push_back((unsigned char)(data[raw_start]));
        raw_start = raw_start + 1;}
      objs[objs_end] = Rcpp::wrap(vec);
      objs_end = objs_end + 1;}
      break;
      case 2: { //boolean
        if(data.size() < raw_start + length) {
          throw ReadPastEnd("logical", raw_start);}
        std::vector<bool> vec;
        for(int i = 0; i < length; i++) {
          vec.push_back((bool)(data[raw_start]));
          raw_start = raw_start + 1;}
        objs[objs_end] = Rcpp::wrap(vec);
        objs_end = objs_end + 1;}
        break;
      case 3: { //integer
        std::vector<int> vec;
        for(int i = 0; i < length; i++) {
          vec.push_back(unserialize_one<int>(data, raw_start));}
        objs[objs_end] = Rcpp::wrap(vec);
        objs_end = objs_end + 1;}
        break;
      case 4: { //long
        std::vector<int> vec;
        for(int i = 0; i < length; i++) {
          vec.push_back(unserialize_one<long>(data, raw_start));}
        objs[objs_end] = Rcpp::wrap(vec);
        objs_end = objs_end + 1;}
        break;
      case 5: { //float
        throw UnsupportedType(vec_type_code);}
        break;
      case 6: { //double
        std::vector<double> vec;
        for(int i = 0; i < length; i++) {
          vec.push_back(unserialize_one<double>(data, raw_start));}
        objs[objs_end] = Rcpp::wrap(vec);
        objs_end = objs_end + 1;}
        break;
      default: {
          throw UnsupportedType(vec_type_code);}}}
  
void unserialize(const raw & data, int & raw_start, Rcpp::List & objs, int & objs_end, unsigned char type_code = 255){
  if(type_code == 255) {
    type_code = get_type(data, raw_start);}
  switch(type_code) {
    case 0: { //raw bytes
      int length = get_length(data, raw_start);
      if(data.size() < raw_start + length) {
        throw ReadPastEnd("raw", raw_start);}
      raw tmp(data.begin() + raw_start, data.begin() + raw_start + length);
      objs[objs_end] = Rcpp::wrap(tmp);
      raw_start = raw_start + length;
      objs_end = objs_end + 1;}
      break;
    case 1: { //byte
      if(data.size() < raw_start + 1) {
        throw ReadPastEnd("byte", raw_start);}
      objs[objs_end] = Rcpp::wrap((unsigned char)(data[raw_start]));
      raw_start = raw_start + 1; 
      objs_end = objs_end + 1;}
      break;
    case 2: { //boolean
      if(data.size() < raw_start + 1) {
        throw ReadPastEnd("logical", raw_start);}
      objs[objs_end] = Rcpp::wrap(bool(data[raw_start]));
      raw_start = raw_start + 1;
      objs_end = objs_end + 1;}
      break;
    case 3: { //integer
      int tmp = unserialize_one<int>(data, raw_start);
      objs[objs_end] = Rcpp::wrap(tmp);
      objs_end = objs_end + 1;}      
      break;
    case 4: { //long
      objs[objs_end] = Rcpp::wrap(unserialize_one<long>(data, raw_start));
      objs_end = objs_end + 1;} 
      break;
    case 5: { //float
      throw UnsupportedType(type_code);}
      break;
    case 6: { //double
      objs[objs_end] = Rcpp::wrap(unserialize_one<double>(data, raw_start));
      objs_end = objs_end + 1;}
      break;
    case 7: { //string
      int length = get_length(data, raw_start);
      if(data.size() < raw_start + length) {
        throw ReadPastEnd("character", raw_start);}
      std::string tmp(data.begin() + raw_start, data.begin() + raw_start + length);
      objs[objs_end] = Rcpp::wrap(tmp);
      raw_start = raw_start + length;
      objs_end = objs_end + 1;}
      break;
    case 8: { //vector
      int length = get_length(data, raw_start);
      Rcpp::List list(length);
      int list_end = 0; 
      for(int i = 0; i < length; i++) {
        unserialize(data, raw_start, list, list_end);}
      objs[objs_end] = Rcpp::wrap(list);
      objs_end = objs_end + 1;}
      break;
    case 9: // list (255 terminated vector)
    case 10: { //map
      throw UnsupportedType(type_code);}
      break;
    case 144: { //R serialization
      int length = get_length(data, raw_start);
      if(data.size() < raw_start + length) {
        throw ReadPastEnd("SEXP", raw_start);}
      Rcpp::Function r_unserialize("unserialize");
      raw tmp(data.begin() + raw_start, data.begin() + raw_start + length);
      objs[objs_end] = r_unserialize(Rcpp::wrap(tmp));
      raw_start = raw_start + length;
      objs_end = objs_end + 1;}
      break;
    case 145: {
      int raw_length = get_length(data, raw_start);
      int vec_type_code  = get_type(data, raw_start);
      int length;
      switch(vec_type_code) {
        case 1:{
          length = raw_length - 1;
          unserialize_fixed(data, raw_start, objs, objs_end, vec_type_code, length);}
        break;
        case 2:{
          length = raw_length - 1;
          unserialize_fixed(data, raw_start, objs, objs_end, vec_type_code, length);}
        break;
        case 3:{
          length = (raw_length - 1)/4;
          unserialize_fixed(data, raw_start, objs, objs_end, vec_type_code, length);}
        break;
        case 4:{
          length = (raw_length - 1)/8;
          unserialize_fixed(data, raw_start, objs, objs_end, vec_type_code, length);}
        break;
        case 6:{
          length = (raw_length - 1 )/8;
          unserialize_fixed(data, raw_start, objs, objs_end, vec_type_code, length);}
        break;
        default: {
          throw UnsupportedType(vec_type_code);}}}
      break;
    case 146: {
      int raw_length = get_length(data, raw_start);
      int length = get_length(data, raw_start);
      Rcpp::List list(length);
      int list_end = 0; 
      for(int i = 0; i < length; i++) {
        unserialize(data, raw_start, list, list_end);}
      Rcpp::Function unlist("unlist");
      objs[objs_end] = unlist(Rcpp::wrap(list));
      objs_end = objs_end + 1;}
      break;
    default: {
      throw UnsupportedType(type_code);}}}

SEXP typed_bytes_reader(SEXP data, SEXP _nobjs){
	Rcpp::NumericVector nobjs(_nobjs);
	Rcpp::List objs(nobjs[0]);
	Rcpp::RawVector tmp(data);
	raw rd(tmp.begin(), tmp.end());
	int raw_start = 0;
	int parsed_raw_start = 0;
	int objs_end = 0;
	while(rd.size() > raw_start && objs_end < nobjs[0]) {
 		try{
      unserialize(rd, raw_start, objs, objs_end);
      parsed_raw_start = raw_start;}
    catch (ReadPastEnd rpe){
      break;}
		catch (UnsupportedType ue) {
      std::cerr << "Unsupported type exception: " << (int)ue.type_code << std::endl;
      return R_NilValue;}
		catch (NegativeLength nl) {
            std::cerr << "Negative length exception" << std::endl;
      return R_NilValue;}}
  Rcpp::List list_tmp(objs.begin(), objs.begin() + objs_end);
	return Rcpp::wrap(
    Rcpp::List::create(
      Rcpp::Named("objects") = Rcpp::wrap(list_tmp),
      Rcpp::Named("length") = Rcpp::wrap(parsed_raw_start)));}

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
void serialize_one(const T & data, unsigned char type_code, raw & serialized) {
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
    serialize_one(data[0], type_code, serialized);}
  else {
    if(native) {
      serialized.push_back(145);
      length_header(data.size() * sizeof(data[0]) + 1, serialized); 
      serialized.push_back(type_code);
      for(typename T::iterator i = data.begin(); i < data.end(); i++) {
        serialize_one(*i, 255, serialized);}}
    else {
      serialized.push_back(8);
      length_header(data.size(), serialized); 
      for(typename T::iterator i = data.begin(); i < data.end(); i++) {
        serialize_one(*i, type_code, serialized);}}}}

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
      std::cerr << "Unsupported type exception: " << (int)ut.type_code << std::endl;
      return R_NilValue;}}
	return Rcpp::wrap(serialized);}	


