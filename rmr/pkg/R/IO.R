  library(inline)
  
  include = "
  #include <vector>
  #include <string>
  #include <iostream>

  typedef std::vector<unsigned char> raw;

  //template <typename T> raw T2raw(T data) {
  //  std::cerr << \"empty template implementation\";
  //  return raw(0);
  //};

  raw T2raw(int data) {
    raw serialized(4);
    serialized[0] = (data >> 24) & 255;
    serialized[1] = (data >> 16) & 255;
    serialized[2] = (data >>  8) & 255;
    serialized[3] = (data      ) & 255;
    return serialized;}

  raw T2raw(unsigned long data) {
    raw serialized(8);
    serialized[0] = (data >> 56) & 255;
    serialized[1] = (data >> 48) & 255;
    serialized[2] = (data >> 40) & 255;
    serialized[3] = (data >> 32) & 255;
    serialized[4] = (data >> 24) & 255;
    serialized[5] = (data >> 16) & 255;
    serialized[6] = (data >>  8) & 255;
    serialized[7] = (data      ) & 255;
    return serialized;}
  
  raw T2raw(double data) {
    union udouble {
      double d;
      unsigned long u;} ud;
    ud.d = data;
    return T2raw(ud.u);}

  raw length_header(int l){
    return T2raw(l);}
  
  template <typename T> void serialize_one(const T & data, unsigned char type_code, raw & serialized) {
    serialized.push_back(type_code);
    raw rd = T2raw(data);
    serialized.insert(serialized.end(), rd.begin(), rd.end());}
  
  template <typename T> void serialize_many(const T & data, unsigned char type_code, raw & serialized){
    serialized.push_back(type_code);
    raw lh(length_header(data.size()));
    serialized.insert(serialized.end(),lh.begin(), lh.end());
    serialized.insert(serialized.end(), data.begin(), data.end());}
  
  void serialize(const SEXP & object, raw & serialized); 

  template <typename T> void serialize_vector(const T & data, unsigned char type_code, raw & serialized){
    serialized.push_back(8);
    raw lh(length_header(data.size()));
    serialized.insert(serialized.end(), lh.begin(), lh.end());
    for(typename T::iterator i = data.begin(); i < data.end(); i++) {
      serialize_one(*i, type_code, serialized);}}

  template <typename T> void serialize_list(const T & data, raw & serialized){
    serialized.push_back(8);
    raw lh(length_header(data.size()));
    serialized.insert(serialized.end(), lh.begin(), lh.end());
    for(typename T::iterator i = data.begin(); i < data.end(); i++) {
      serialize(Rcpp::wrap(*i), serialized);}}

  void serialize(const SEXP & object, raw & serialized) {
    Rcpp::RObject robj(object);
    switch(robj.sexp_type()) {
      case 24: {//raw
        Rcpp::RawVector data(object);
        if(data.size() == 1){
          serialize_one(data[0], 1, serialized);}
        else {
          serialize_many(data, 0, serialized);}}
        break;
      case 16: { //character
        Rcpp::CharacterVector data(object);
        if(data.size() == 1) {
          serialize_many(data[0], 7, serialized);}
        else {
          serialized.push_back(8);
          raw lh(length_header(data.size()));
          serialized.insert(serialized.end(), lh.begin(), lh.end());
          for(int i = 0; i < data.size(); i++) {
            serialize_many(data[i], 7, serialized);}}}
        break; 
      case 10: { //logical
        Rcpp::LogicalVector data(object);
        if(data.size() == 1) {
          serialize_one(data[0], 2, serialized);}
        else {
          serialize_vector(data, 2, serialized);}}
        break;
      case 14: { //numeric
        Rcpp::NumericVector data(object);
        if(data.size() == 1) {
          serialize_one(data[0], 6, serialized);}
        else {
          serialize_vector(data, 6, serialized);}}
        break;
      case 13: { //factor, integer
        Rcpp::IntegerVector data(object);
        if(data.size() == 1) {
          serialize_one(data[0], 3, serialized);}
        else {
          serialize_vector(data, 3, serialized);}
        }
        break;
      case 19: { //list
        Rcpp::List data(object);
        serialize_list(data, serialized);}
        break;
      default:
      std::cerr << \"object type not supported: \" << robj.sexp_type();}}
  "
  src = "
  raw serialized(0);
  serialize(object, serialized);
  return Rcpp::wrap(serialized);
  "
  
  typed.bytes.Cpp.writer = cxxfunction(signature(object = "any"),  src, plugin = "Rcpp", includes=include)
