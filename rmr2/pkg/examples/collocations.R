library(rmr2)

ngram.format = 
  make.input.format(
    format="csv", 
    quote = NULL, 
    sep = "\t", 
    comment.char = "",
    col.names = c("ngram", "year", "count", "pages", "books"),
    stringsAsFactors = FALSE)

ngram.parse = 
  function(ngram.data) {
    ngram.split = suppressWarnings(do.call(rbind, strsplit(paste(ngram.data$ngram, "     "), " "))[,1:5])
    filter = ngram.split[,ncol(ngram.split)] != "" 
    cbind(ngram.data[,-1], ngram.split, stringsAsFactors = FALSE)[filter,]}

map.fun = 
  function(k, v) {
    data = ngram.parse(v)
    keyval(as.matrix(data[, c("year", "1", names(data)[ncol(data)])]), data$count)}

reduce.fun = 
  function(k,vv) {
    vv = split(vv, as.data.frame(k), drop = TRUE)
    keyval(names(vv), psum(vv))}

system.time({
  zz = 
    mapreduce(
      "/user/ngrams/googlebooks-eng-all-5gram-20090715-159.csv",
      #"../RHadoop.data/ngrams/1000000.csv",      
      input.format = ngram.format, 
      map = map.fun, 
      reduce = reduce.fun,
      vectorized.reduce = TRUE,
      in.memory.combine = TRUE,
      combine = FALSE)})