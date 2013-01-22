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
    ngram.split = do.call(rbind, strsplit(paste0(ngram.data$ngram, "     "), " "))[,1:5]
    filter = ngram.split[,ncol(ngram.split)] != "" 
    cbind(ngram.data[,-1], ngram.split, stringsAsFactors = FALSE)[filter,]}
  
# 
# def map(record):
#   (ngram, year, count) = unpack(record)
# // ensure word1 has the lexicographically first word:
#   (word1, word2) = sorted(ngram[first], ngram[last])
# key = (word1, word2, year)
# emit(key, count)
# 
# def reduce(key, values):
#   emit(key, sum(values))


map.fun = 
  function(k,v) {
    increment.counter("rmr","map calls")
    #rmr.str(v)
    data = ngram.parse(v)
    #rmr.str(data)
    sums = 
      sapply(
        split(
          data$count,
          data[,c(1, 5, ncol(data))],
          drop = TRUE), 
        sum)
    #rmr.str(sums)
    keyval(names(sums), sums)}

system.time({
  zz = 
    mapreduce(
#      "/user/ngrams/googlebooks-eng-all-5gram-20090715-756.csv", 
      "../RHadoop.data/ngrams/1000000.csv",      
      input.format=ngram.format, 
      map = map.fun, 
      reduce = function(k,vv) {#rmr.str(k); #rmr.str(vv); 
        keyval(k, sum(vv))}, 
      combine = TRUE)
  })