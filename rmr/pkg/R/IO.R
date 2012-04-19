  library(inline)
  
  include = "
  #include <vector>
  #include <string>
  
  std::vector<unsigned char> int2raw(int i) {
  std::vector<unsigned char> c;
    c.resize(4);
    c[0] = (i >> 24) & 255;
    c[1] = (i >> 16) & 255;
    c[2] = (i >>  8) & 255;
    c[3] = (i      ) & 255;
    return c;}
  
  std::vector<unsigned char> long2raw(long l){}
  
  std::vector<unsigned char> length_header(unsigned int l){
    return int2raw(l);}
  
  template <typename T> std::vector<unsigned char> serialize_one(T data, unsigned char type_code) {
    std::vector<unsigned char> serialized(0);
    serialized.push_back(type_code);
    serialized.push_back(data[0]);
    return serialized;}
  
  template <typename T> std::vector<unsigned char> serialize_many(T data, unsigned char type_code){
    std::vector<unsigned char> serialized(0);
    serialized.push_back(type_code);
    std::vector<unsigned char>lh(length_header(data.size()));
    serialized.insert(serialized.end(),lh.begin(), lh.end());
    serialized.insert(serialized.end(), data.begin(), data.end());
    return serialized;}
  
  
  std::vector<unsigned char> serialize(SEXP object) {
    Rcpp::RObject robj(object);
    std::vector<unsigned char> serialized(0);
    switch(robj.sexp_type()) {
      case 24: {//raw
        Rcpp::RawVector rawvec(object);
        if(rawvec.size() == 1){
          serialized = serialize_one(rawvec, 1);}
        else {
          serialized = serialize_many(rawvec, 0);}}
        break;
      case 16: { //character
        Rcpp::CharacterVector charvec(object);
        if(charvec.size() == 1) {
          std::string tmp = as<std::string>(charvec);
          serialized.push_back(7);
          std::vector<unsigned char> lh(length_header(tmp.size()));
          serialized.insert(serialized.end(), lh.begin(), lh.end());
          serialized.insert(serialized.end(), tmp.begin(), tmp.end());}
        else {
          serialized.push_back(8);
          std::vector<unsigned char> lh = length_header(charvec.size());
          serialized.insert(serialized.end(), lh.begin(), lh.end());
          for(Rcpp::CharacterVector::iterator i = charvec.begin(); i < charvec.end(); i++){
            serialized.push_back(7);
            lh = length_header(i->size());
            serialized.insert(serialized.end(), lh.begin(), lh.end());
            serialized.insert(serialized.end(), i->begin(), i->end());}}}
        break; 
      case 10: //logical
      case 14: //numeric
      case 13: //factor
      case 19: //list
      default:
      std::cerr << \"object type not supported\";
      exit(-1);
    }
    
    return serialized;}
  "
  src = "
  return Rcpp::wrap(serialize(object));
  "
  
  typed.bytes.Cpp.writer = cxxfunction(signature(object = "any"),  src, plugin = "Rcpp", includes=include)
