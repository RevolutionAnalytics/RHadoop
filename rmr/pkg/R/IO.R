library(inline)

include = "
#include <vector>

std::vector<unsigned char> length_header(unsigned int l){
  std::vector<unsigned char> c;
  c.resize(4);
  c[0] = (l >> 24) & 255;
  c[1] = (l >> 16) & 255;
  c[2] = (l >>  8) & 255;
  c[3] = (l      ) & 255;
  return c;
}
"
src = "
Rcpp::RObject robj(object);
std::vector<unsigned char> serialized(0);
switch(robj.sexp_type()) {
  case 24: { //raw
    Rcpp::RawVector rawvec(object);
    serialized.push_back(0);
    std::vector<unsigned char>lh(length_header(rawvec.size()));
    serialized.insert(serialized.end(),lh.begin(), lh.end());
    serialized.insert(serialized.end(), rawvec.begin(), rawvec.end());}
    break;
  case 16: //character
  case 10: //logical
  case 14: //numeric
  case 13: //factor
  case 19: //list
  default:
  std::cerr << \"object type not supported\";
  exit(-1);
}
return Rcpp::wrap(serialized);"

typed.bytes.Cpp.writer = cxxfunction(signature(object = "any"),  src, plugin = "Rcpp", includes=include)
