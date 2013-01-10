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
    ngram.split = str_split_fixed(ngram.data$ngram, " ", 5)
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
    data = ngram.parse(v)
    sums = 
      sapply(
        split(
          data$count,
          data[,c(1, 5, ncol(data))],
          drop = TRUE), 
        sum)
    keyval(names(sums), sums)}

system.time({
  zz = 
    mapreduce(
      "../RHadoop.data/ngrams/googlebooks-eng-all-5gram-20090715-519.csv", 
      input.format=ngram.format, 
      map = map.fun, 
      reduce = function(k,vv) keyval(k, sum(vv)), 
      combine = T)
  })