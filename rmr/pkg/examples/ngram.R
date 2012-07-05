# Start cluster
# $WHIRR_HOME/bin/whirr  launch-cluster --config ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/hadoop-ec2-lzo.properties 2>&1 
# $WHIRR_HOME/bin/whirr  run-script --script ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/rmr-1.3.sh  --config ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/hadoop-ec2-lzo.properties
# $WHIRR_HOME/bin/whirr  run-script --script ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/lzo.sh  --config ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/hadoop-ec2-lzo.properties

library(rmr)
library(robustbase)
#using 1.2.2
#fake data
fake.size = 2000000
writeLines(
  apply(
    cbind(
      sample(sapply(1:20000, function(x) substr(digest(x),start=1,stop=3)), fake.size, replace = TRUE), 
      sample(1800:1819, fake.size, replace = T),
      sample (1:200, fake.size, replace=T), 
      sample (1:200, fake.size, replace=T), 
      sample (1:200, fake.size, replace=T)),
    1, 
    function(x)paste(x, collapse = "\t")), 
  file("/tmp/fake-ngram-data", "w"))
  
# fake data
source = "/tmp/fake-ngram-data"
# rmr.options.set(backend = "local")

# real data
# do  distcp and then scatter
# hadoop distcp s3n://$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY@datasets.elasticmapreduce/ngrams/books/20090715/eng-all/1gram/ hdfs:///user/antonio/

#source = "hdfs:///user/antonio/1gram/data"
rmr.options.set(backend = "hadoop")

ngram.format = function(lines){
  data = as.data.frame(do.call(rbind, strsplit(unlist(lines), "\t"))[,1:3])
  names(data) = c("ngram", "year", "count")
  data$year = as.integer(as.character(data$year))
  data$count = as.integer(as.character(data$count))
  data}

#filter and normalize n-grams
filter.map = function(k, lines) {
  data = ngram.format(lines)
  data$ngram = tolower(data$ngram)
  data = data[regexpr("^[a-z+'-]+$", data$ngram) > 0,]
  keyval(data$ngram[data$year > 1700], data[data$year > 1700, -1], vectorized = TRUE)}

filter.reduce = 
  function(ngram, year.counts) {
    names(year.counts) = c("year", "counts")
    keyval(ngram, aggregate(year.counts$counts, by = list(year.counts$year), sum))}

filter.norm.data = 
  mapreduce(input = source,
            input.format = "text", #comment this on real data
            map = filter.map,
            reduce = filter.reduce,
            vectorized = list(map = TRUE),
            structured = list(reduce = TRUE))
  
totals.map = 
  function(ngram, year.counts) {
    names(year.counts) = c("year", "counts")
    keyval(year.counts$year + sample(seq(from=0,to=.9, by = .01), nrow(year.counts), replace = TRUE)
           , 
           year.counts$counts,
           vectorized = TRUE)}

totals.reduce = 
  function(year, counts) keyval(floor(year), sum(unlist(counts), na.rm = TRUE))
  
#group counts by year and find totals
year.totals = 
from.dfs(
  mapreduce(input = filter.norm.data,
            map = totals.map,
            reduce = totals.reduce,
            combine = TRUE),
  structured = TRUE)

names(year.totals) = c("year", "count")
yt = tapply(year.totals$count, year.totals$year, function(x) sum(x))
year.totals = data.frame(year = as.integer(names(yt)), count = yt)
rownames(year.totals) = year.totals$year

#year.totals = year.totals[-(1:77),]

#group by ngram and compute p-values

outlier.map = 
  function(ngram, count.sparse) {
    names(count.sparse) = c("year", "count")
    rownames(count.sparse) = count.sparse$year
    count = rep(min(count.sparse$count) - 1, nrow(year.totals))
    count[count.sparse$year - min(year.totals$year) + 1] = count.sparse$count
    log.freq = log((count + 1)/(year.totals$count + 1))
    keyval(year.totals$year[-1] + sample(seq(from=0,to=.9, by = .1), nrow(year.totals) - 1, replace = TRUE)
           , cbind(ngram, log.freq[-length(log.freq)], 
                                     log.freq[-1]),
           vectorized = TRUE)}

outlier.reduce = function(year, log.freqs) {
  keyval(NULL, unique(log.freqs[!adjOutlyingness(log.freqs[,2:3])$nonOut,1]))}

outlier.ngrams = c()
outlier.ngrams[
  unique(
    unlist(
        from.dfs(
          mapreduce(input = filter.norm.data,
                    map = outlier.map,
                    reduce = outlier.reduce,
                    structured = list(reduce = TRUE)))))] = TRUE

ngram.filter = 
  function(ngram) !is.na(outlier.ngrams[ngram])
  

graph.data = 
  from.dfs(
    mapreduce(
      input = filter.norm.data,
      map = function(ngram, data) if(ngram.filter(ngram)) keyval(ngram, data)))
#visualization

library(googleVis)
qw = function(...) as.character(match.call())[-1]
graph.data.frame = do.call(rbind, lapply(graph.data, function(x) cbind(x$key, x$val[-1,], x$val[-nrow(x$val),])))
names(graph.data.frame) = qw(id,time,count, time.1, count.1)
graph.data.frame$average = with(graph.data.frame, log(count/year.totals[as.character(time),1] * count.1/year.totals[as.character(time.1),1])/2)
graph.data.frame$difference = with(graph.data.frame, log(count/year.totals[as.character(time),1] / count.1/year.totals[as.character(time.1),1])/2)
M <- gvisMotionChart(graph.data.frame[,qw(id,time,average,difference)], options = list(height = 1000, width = 2000)); cat(M$html$chart, file="~/Desktop/tmp.html")