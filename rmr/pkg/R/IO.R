  library(inline)
  
  lapply.nrecs = function(..., nrecs) {
    out = lapply(...)
    if(nrecs == 1) out[[1]] else out}
  
  native.text.input.format = function(con, nrecs) {
    lines  = readLines(con, nrecs)
    if (length(lines) == 0) NULL
    else {
      splits = strsplit(lines, "\t")
      deser.one = function(x) unserialize(charToRaw(gsub("\\\\n", "\n", x)))
      keyval(lapply.nrecs(splits, function(x) de(x[1]), nrecs = nrecs),
             lapply.nrecs(splits, function(x) de(x[2]), nrecs = nrecs),
             vectorized = nrecs == 1)}}
  
  native.text.output.format = function(k, v, con, vectorized) {
    ser = function(x) gsub("\n", "\\\\n", rawToChar(serialize(x, ascii=T, conn = NULL)))
    ser.pair = function(k,v) paste(ser(k), ser(v), sep = "\t")
    out = 
      if(vectorized)
        mapply(ser.pair, k, v)
    else ser.pair(k,v)
    writeLines(out, con = con, sep = "\n")}
  
  json.input.format = function(con, nrecs) {
    lines = readLines(con, nrecs)
    if (length(lines) == 0) NULL
    else {
      splits =  strsplit(lines, "\t")
      if(length(splits[[1]]) == 1)  keyval(NULL, lapply.nrecs(splits, function(x) fromJSON(x[1], asText = TRUE), nrecs = nrecs))
      else keyval(lapply.nrecs(splits, function(x) fromJSON(x[1], asText = TRUE), nrecs = nrecs), 
                  lapply.nrecs(splits, function(x) fromJSON(x[2], asText = TRUE), nrecs = nrecs),
                  vectorized = nrecs == 1)}}
  
  json.output.format = function(k, v, con, vectorized) {
    ser = function(k, v) paste(gsub("\n", "", toJSON(k, .escapeEscapes=TRUE, collapse = "")),
                               gsub("\n", "", toJSON(v, .escapeEscapes=TRUE, collapse = "")),
                               sep = "\t")
    out = 
      if(vectorized)
        mapply(k,v, ser)
    else
      ser(k,v)
    writeLines(out, con = con, sep = "\n")}
  
  text.input.format = function(con, nrecs) {
    lines = readLines(con, nrecs)
    if (length(lines) == 0) NULL
    else keyval(NULL, lines, vectorized = nrecs == 1)}
  
  text.output.format = function(k, v, con, vectorized) {
    ser = function(k,v) paste(k, v, collapse = "", sep = "\t")
    out = if(vectorized)
      mapply(ser, k, v)
    else
      ser(k,v)
    writeLines(out, sep = "\n", con = con)}
  
  csv.input.format = function(..., nrecs) function(con) {
    df = 
      tryCatch(
        read.table(file = con, nrows = nrecs, header = FALSE, ...),
        error = function(e) NULL)
    if(is.null(df) || dim(df)[[1]] == 0) NULL
    else keyval(NULL, df, vectorized = nrecs == 1)}
  
  csv.output.format = function(...) function(k, v, con, vectorized) 
    # this is vectorized only, need to think what that means
    write.table(file = con, 
                x = if(is.null(k)) v else cbind(k,v), 
                ..., 
                row.names = FALSE, 
                col.names = FALSE)
  
  typed.bytes.reader = 
    function(buf.size = 1000) {
      raw.buffer = raw()
      buffered.readBin = function(con, what, n = 1, size = NULL, ...) {
        if(what == "raw") size = 1
        raw.size = n * size
        if(raw.size <= length(raw.buffer)){
          retval = readBin(raw.buffer[1:(size*n)], what, n, size, ...)
          raw.buffer <<- raw.buffer[-(1:(size*n))]
          retval
        }
        else {
          raw.buffer <<- c(raw.buffer, readBin(con, "raw", buf.size, ...))
          if(length(raw.buffer) > 0) buffered.readBin(con, what, n, size, ...)
          else NULL
        }
      }
      reader = function (con, type.code = NULL) {
        r = function(...) {
          buffered.readBin(con, endian = "big", signed = TRUE, ...)}
        read.code = function() (256 + r(what = "integer", n = 1, size = 1)) %% 256
        read.length = function() r(what = "integer", n= 1, size = 4)
        tbr = function() reader(con)[[1]]
        two55.terminated.list = function() {
          ll = list()
          code = read.code()
          while(length(code) > 0 && code != 255) {
            ll = append(ll,  reader(con, code))
            code = read.code()}
          ll}#quadratic, fix later
        
        ##      if(is.raw(con)) 
        ##        con = rawConnection(con, open = "r") 
        if (is.null(type.code)) type.code = read.code()
        if(length(type.code) > 0) {
          list(switch(as.character(as.integer(type.code)), 
                      "0" = r("raw", n = read.length()), 
                      "1" = r("raw"),                    
                      "2" = r("logical", size = 1),      
                      "3" = r("integer", size = 4),      
                      "4" = r("integer", size = 8),      
                      "5" = r("numeric", size = 4),      
                      "6" = r("numeric", size = 8),      
                      "7" = rawToChar(r("raw", n = read.length())), 
                      "8" = replicate(read.length(), tbr(), simplify=FALSE), 
                      "9" = two55.terminated.list(), 
                      "10" = replicate(read.length(), keyval(tbr(), tbr()), simplify = FALSE),
                      "11" = r("integer", size = 2), #this and the next implemented only in hive, silly
                      "12" = NULL,
                      "144" = unserialize(r("raw", n = read.length()))))} 
        else NULL}
      reader}
  
  typed.bytes.writer = function(value, con, native  = FALSE) {
    w = function(x, size = NA_integer_) writeBin(x, con, size = size, endian = "big")
    write.code = function(x) w(as.integer(x), size = 1)
    write.length = function(x) w(as.integer(x), size = 4)
    tbw = function(x) typed.bytes.writer(x, con)
    if(native) {
      bytes = serialize(value, NULL)
      write.code(144); write.length(length(bytes)); w(bytes)
    }
    else{
      if(is.list(value) && all(sapply(value, is.keyval))) {
        write.code(10)
        write.length(length(value))
        lapply(value, function(kv) {lapply(kv, tbw)})}
      else {
        if(length(value) == 1) {
          switch(class(value), 
                 raw = {write.code(1); w(value)}, 
                 logical = {write.code(2); w(value, size = 1)}, 
                 integer = {write.code(3); w(value)}, 
                 #doesn't happen in R integer = {write.code(4); w(value)}, 
                 #doesn't happen in R numeric = {write.code(5); w(value)}, 
                 numeric = {write.code(6); w(value)}, 
                 character = {write.code(7); write.length(nchar(value)); writeChar(value, con, eos = NULL)}, 
                 factor = {value = as.character(value)
                           write.code(7); write.length(nchar(value)); writeChar(value, con, eos = NULL)},
                 list = {write.code(8); write.length(1); tbw(value[[1]])},
                 stop("not implemented yet"))}
        else {
          switch(class(value), 
                 raw = {write.code(0); write.length(length(value)); w(value)}, 
                 #NULL = {write.code(12); write.length(0)}, #this spec was added by the hive folks, but not in streaming HIVE-1029
                 {write.code(8); write.length(length(value)); lapply(value, tbw)})}}}
    TRUE}
  
  typed.bytes.input.format = function() {
    tbr = typed.bytes.reader()
    function(con, nrecs) {
      out = replicate(2*nrecs, tbr(con), simplify = FALSE)
      out = out[!sapply(out, is.null)]
      if(length(out) == 0) NULL
      else {
        if(nrecs == 1)
          keyval(out[[1]][[1]],out[[2]][[1]], vectorized = FALSE)
        else
          keyval(out[2*(1:length(out)) - 1], out[2*(1:length(out))], vectorized = TRUE)}}}
  
  typed.bytes.output.format = function(k, v, con, vectorized) {
    ser = function(k,v) {
      typed.bytes.writer(k, con)
      typed.bytes.writer(v, con)
    }
    if(vectorized)
      mapply(ser, k, v)
    else
      ser(k,v)}
  
  native.input.format = typed.bytes.input.format
  
  native.output.format = function(k, v, con, vectorized){
    ser.native = function(k,v) {
      typed.bytes.writer(k, con, TRUE)
      typed.bytes.writer(v, con, TRUE)
    }
    ser.non.native = function(k,v) {
      typed.bytes.writer(k, con, FALSE)
      typed.bytes.writer(v, con, FALSE)
    }
    if(vectorized)
      mapply(ser.non.native, k, v)
    else
      ser.native(k,v)}
  
  include = "
  #include <vector>
  #include <string>
  #include <iostream>

  typedef std::vector<unsigned char> raw;

  raw T2raw(unsigned char data) {
    return data;}

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
  Rcpp::List kk(k);
  Rcpp::List vv(v);
  Rcpp::List::iterator i = kk.begin();
  Rcpp::List::iterator j = vv.begin();
  for(; i < kk.end() && j < vv.end(); i++, j++) {
    serialize(Rcpp::wrap(*i), serialized);
    serialize(Rcpp::wrap(*j), serialized);
  }
  return Rcpp::wrap(serialized);
  "
  
typed.bytes.Cpp.writer = cxxfunction(signature(k = "any", v = "any"),  src, plugin = "Rcpp", includes = include)
  
  include = "
  #include <deque>
  #include <iostream>
  
  typedef std::deque<unsigned char> raw;

  int raw2int(raw & data) {
    int retval =  ((data[0] & 255) << 24) + 
                  ((data[1] & 255) << 16) +
                  ((data[2] & 255) <<  8) + 
                  ((data[3] & 255)      );
    data.erase(data.begin(), data.begin() + 4);
    return retval;}

//   long raw2long(raw & data) {
//     long retval = 
//            (((long long) data[0] & 255) << 56) + 
//            (((long long) data[1] & 255) << 48) +
//            (((long long) data[2] & 255) << 40) + 
//            (((long long) data[3] & 255) << 32) +
//            (((long long) data[4] & 255) << 24) +
//            (((long long) data[5] & 255) << 16) +
//            (((long long) data[6] & 255) <<  8) +
//            (((long long) data[7] & 255)      );
//     data.erase(data.begin(), data.begin() + 8);
//     return retval;}

  double raw2double(raw & data) {} 
 
  int get_length(raw & data) {
    return raw2int(data);}

  unsigned char get_type(raw & data) {
    unsigned char retval  = data[0];
    data.pop_front();
    return retval;}

  int unserialize(raw & data, Rcpp::List & objs){
    unsigned char type_code = get_type(data);
    switch(type_code) {
      case 0: {
        int length = get_length(data);
        raw tmp(data.begin(), data.begin() + length);
        objs.insert(objs.end(), Rcpp::wrap(tmp));
        data.erase(data.begin(), data.begin() + length);}
      break;
      case 1: {
        objs.push_back(Rcpp::wrap((unsigned char)(data[0])));
        data.pop_front(); }
      break;
      case 2: {
        objs.push_back(Rcpp::wrap(bool(data[0])));
        data.pop_front();}
      break;
      case 3: {
        objs.push_back(Rcpp::wrap(raw2int(data)));}      
      break;
      case 4:
      case 5: {
        std::cerr << type_code << \" type code not supported\" << std::endl;}
      break;
      case 6: {
        objs.push_back(Rcpp::wrap(raw2double(data)));}
      break;
      case 7: {
        int length = get_length(data);
        std::string tmp(data.begin(), data.begin() + length);
        objs.insert(objs.end(), Rcpp::wrap(tmp));
        data.erase(data.begin(), data.begin() + length);}
      break;
      case 8: {
        int length = get_length(data);
        for(int i = 0; i < length; i++) {
          unserialize(data, objs);}}
      break;
      case 9: 
      case 10: {
        std::cerr << type_code << \" type code not supported\" << std::endl;}
      break;
      case 144: {
        std::cerr << type_code << \" type code not supported\" << std::endl;}
      break;
      default: {
        std::cerr << \"Unknown type code \" << type_code << std::endl;}}}
  "
  src = "
  Rcpp::List kk(0);
  Rcpp::RawVector tmp(data);
  raw rd(tmp.begin(), tmp.end());
  unserialize(rd, kk);
  return Rcpp::wrap(kk);
  "
  
  typed.bytes.Cpp.reader = cxxfunction(signature(data = "raw"), src, plugin = "Rcpp", includes = include)
