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

typedef std::deque<unsigned char> raw;
#include <string>
#include <iostream>

class ReadPastEnd {
public:
  ReadPastEnd(){};};

class UnsupportedType{
public:
  unsigned char type_code;
  UnsupportedType(unsigned char _type_code){
    type_code = _type_code;};};

class NegativeLength {
public:
  NegativeLength(){};};

int raw2int(const raw & data, int & start) {
  if(data.size() < start + 4) {
    throw ReadPastEnd();}
  int retval = 0;
  for (int i = 0; i < 4; i ++) {
    retval = retval + ((data[start + i] & 255) << (8*(3 - i)));}
  start = start + 4;
  return retval;}

long raw2long(const raw & data, int & start) {
  if(data.size() < start + 8) {
    throw ReadPastEnd();  }
  long retval = 0;
  for(int i = 0; i < 8; i++) {
    retval = retval + (((long) data[start + i] & 255) << (8*(7 - i)));}
  start = start + 8; 
  return retval;}

double raw2double(const raw & data, int & start) {
  union udouble {
    double d;
    unsigned long u;} ud;
  ud.u = raw2long(data, start);
  return ud.d;} 

int get_length(const raw & data, int & start) {
  int len = raw2int(data, start);
  if(len < 0) {
  	throw NegativeLength();}
  return len;}

unsigned char get_type(const raw & data, int & start) {
  if(data.size() < start + 1) {
    throw ReadPastEnd();}
  unsigned char retval  = data[start];
  start = start + 1;
  return retval;}

void unserialize(const raw & data, int & raw_start, Rcpp::List & objs, int & objs_end, unsigned char type_code = 255){
  if(type_code == 255) {
    type_code = get_type(data, raw_start);}
  switch(type_code) {
    case 0: { //raw bytes
      int length = get_length(data, raw_start);
      if(data.size() < raw_start + length) {
        throw ReadPastEnd();}
      raw tmp(data.begin() + raw_start, data.begin() + raw_start + length);
      objs[objs_end] = Rcpp::wrap(tmp);
      raw_start = raw_start + length;
      objs_end = objs_end + 1;}
      break;
    case 1: { //byte
      if(data.size() < raw_start + 1) {
        throw ReadPastEnd();}
      objs[objs_end] = Rcpp::wrap((unsigned char)(data[raw_start]));
      raw_start = raw_start + 1; 
      objs_end = objs_end + 1;}
      break;
    case 2: { //boolean
      if(data.size() < raw_start + 1) {
        throw ReadPastEnd();}
      objs[objs_end] = Rcpp::wrap(bool(data[raw_start]));
      raw_start = raw_start + 1;
      objs_end = objs_end + 1;}
      break;
    case 3: { //integer
      objs[objs_end] = Rcpp::wrap(raw2int(data, raw_start));
      objs_end = objs_end + 1;}      
      break;
    case 4: { //long
      objs[objs_end] = Rcpp::wrap(raw2long(data, raw_start));
      objs_end = objs_end + 1;} 
      break;
    case 5: { //float
      throw UnsupportedType(type_code);}
      break;
    case 6: { //double
      objs[objs_end] = Rcpp::wrap(raw2double(data, raw_start));
      objs_end = objs_end + 1;}
      break;
    case 7: { //string
      int length = get_length(data, raw_start);
      if(data.size() < raw_start + length) {
        throw ReadPastEnd();}
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
        throw ReadPastEnd();}
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
          length = raw_length - 1;}
        break;
        case 2:{
          length = raw_length - 1;}
        break;
        case 3:{
          length = (raw_length - 1)/4;}
        break;
        case 4:{
          length = (raw_length - 1)/8;}
        break;
        case 6:{
          length = (raw_length -1 )/8;}
        break;
        default: {
          throw UnsupportedType(vec_type_code);}}
      Rcpp::List list(length);
      int list_end = 0; 
      for(int i = 0; i < length; i++) {
        unserialize(data, raw_start, list, list_end, vec_type_code);}
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
	while(rd.size() > raw_start, objs_end < nobjs[0]) {
 		try{
      unserialize(rd, raw_start, objs, objs_end);
      parsed_raw_start = raw_start;}
    catch (ReadPastEnd rpe){
      break;}
		catch (UnsupportedType ue) {
      std::cerr << "Unsupported type: " << (int)ue.type_code << std::endl;
      return R_NilValue;}
		catch (NegativeLength nl) {
      std::cerr << "Negative length Exception" << std::endl;
      return R_NilValue;}}
  Rcpp::List list_tmp(objs.begin(), objs.begin() + objs_end);
	return Rcpp::wrap(Rcpp::List::create(
                                       Rcpp::Named("objects") = Rcpp::wrap(list_tmp),
                                       Rcpp::Named("length") = Rcpp::wrap(parsed_raw_start)));}




void T2raw(unsigned char data, raw & serialized) {
  serialized.push_back(data);}

void T2raw(int data, raw & serialized) {
  for(int i = 0; i < 4; i++) {
    serialized.push_back((data >> (8*(3 - i))) & 255);}}

void T2raw(unsigned long data, raw & serialized) {
  for(int i = 0; i < 8; i++) {  
    serialized.push_back((data >> (8*(7 - i))) & 255);}}

void T2raw(double data, raw & serialized) {
  union udouble {
    double d;
    unsigned long u;} ud;
  ud.d = data;
  T2raw(ud.u, serialized);}

void length_header(int len, raw & serialized){
  if(len < 0) {
  	throw NegativeLength();}
  T2raw(len, serialized);}

template <typename T> void serialize_one(const T & data, unsigned char type_code, raw & serialized) {
  if(type_code != 255) serialized.push_back(type_code);
  T2raw(data, serialized);}

template <typename T> void serialize_many(const T & data, unsigned char type_code, raw & serialized){
  serialized.push_back(type_code);
  length_header(data.size(), serialized);
  serialized.insert(serialized.end(), data.begin(), data.end());}

void serialize(const SEXP & object, raw & serialized, bool native); 

template <typename T> void serialize_vector(const T & data, unsigned char type_code, raw & serialized, bool native){
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

template <typename T> void serialize_list(const T & data, raw & serialized){
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
  switch(robj.sexp_type()) {
  	case NILSXP: {
      if(native) {
        serialize_native(object, serialized);}
      else {
    	  throw UnsupportedType(NILSXP);}}
  	  break;
    case RAWSXP: {//raw
      Rcpp::RawVector data(object);
      serialize_many(data, 0, serialized);}
      break;
    case STRSXP: { //character
      if(native) {
        serialize_native(object, serialized);}
      else {
        Rcpp::CharacterVector data(object);
        serialized.push_back(8);
        length_header(data.size(), serialized);
        for(int i = 0; i < data.size(); i++) {
          serialize_many(data[i], 7, serialized);}}}
      break; 
    case LGLSXP: { //logical
      Rcpp::LogicalVector data(object);
      serialize_vector(data, 2, serialized, native);}
      break;
    case REALSXP: { //numeric
      Rcpp::NumericVector data(object);
      serialize_vector(data, 6, serialized, native);}
      break;
    case INTSXP: { //factor, integer
      Rcpp::IntegerVector data(object);
      serialize_vector(data, 3, serialized, native);}
      break;
    case VECSXP: { //list
      if(native){
        serialize_native(object, serialized);}
      else {
        Rcpp::List data(object);
        serialize_list(data, serialized);}}
      break;
    default: {
      if(native){
        serialize_native(object, serialized);}
      else {
        throw UnsupportedType(robj.sexp_type());}}}}

SEXP typed_bytes_writer(SEXP objs, SEXP native){
	raw serialized(0);
	Rcpp::List objects(objs);
  Rcpp::LogicalVector is_native(native);
	for(Rcpp::List::iterator i = objects.begin(); i < objects.end(); i++) {
    try{
      serialize(Rcpp::wrap(*i), serialized, is_native[0]);}
    catch(UnsupportedType ut){
      return R_NilValue;}}
	return Rcpp::wrap(serialized);}	


