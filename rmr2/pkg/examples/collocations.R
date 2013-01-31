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
  function(k,v) {
    data = ngram.parse(v)
    sums = 
      sapply(
        split(
          data$count,
          data[,c(1, 5, ncol(data))],
          drop = TRUE), 
        sum)
    keyval(names(sums), unname(sums))}

  system.time({
    zz = 
      mapreduce(
  #      "/user/ngrams/googlebooks-eng-all-5gram-20090715-734.csv",
        "../RHadoop.data/ngrams/10000.csv",      
        input.format=ngram.format, 
        map = map.fun, 
        reduce = function(k,vv) {
          keyval(k, psum(vv))},
        vectorized.reduce= TRUE,
        combine = FALSE)
    })