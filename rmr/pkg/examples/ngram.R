# Start cluster
# $WHIRR_HOME/bin/whirr  launch-cluster --config ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/hadoop-ec2-lzo.properties 2>&1 
# $WHIRR_HOME/bin/whirr  run-script --script ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/rmr-master.sh  --config ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/hadoop-ec2-lzo.properties
# $WHIRR_HOME/bin/whirr  run-script --script ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/lzo.sh  --config ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/hadoop-ec2-lzo.properties

library(rmr)
#using 1.2.2
#fake data
fake.size = 1000000
writeLines(
  apply(
    cbind(
      sample(sapply(1:10000, function(x) substr(digest(x),start=1,stop=3)), fake.size, replace = TRUE), 
      sample(1800:1819, fake.size, replace = T),
      sample (1:20, fake.size, replace=T), 
      sample (1:20, fake.size, replace=T), 
      sample (1:20, fake.size, replace=T)),
    1, 
    function(x)paste(x, collapse = "\t")), 
  file("/tmp/fake-ngram-data", "w"))
  
# fake data
# source = "/tmp/fake-ngram-data"
# rmr.options.set(backend = "local")

# real data
# do  distcp and then scatter
# hadoop distcp s3n://$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY@datasets.elasticmapreduce/ngrams/books/20090715/eng-all/1gram/ hdfs:///user/antonio/

source = "hdfs:///user/antonio/1gram/data"
rmr.options.set(backend = "hadoop")

ngram.format = function(lines){
  data = as.data.frame(do.call(rbind, strsplit(lines, "\t"))[,1:3])
  names(data) = c("ngram", "year", "count")
  data$year = as.integer(as.character(data$year))
  data$count = as.integer(as.character(data$count))
  data}

#filter and normalize n-grams
filter.map = function(k, lines) {
  data = ngram.format(lines)
  data$ngram = tolower(data$ngram)
  data = data[regexpr("^[a-z+'-]+$", data$ngram) > 0,]
  keyval(data$ngram, data[,-1], vectorized = TRUE)}

filter.reduce = function(ngram, year.counts) {
  names(year.counts) = c("year", "counts")
  keyval(ngram, aggregate(year.counts$counts, by = list(year.counts$year), sum))}

filter.norm.data = 
mapreduce(input = source,
          input.format = "text", #comment this on real data
          map = filter.map,
          reduce = filter.reduce,
          vectorized = list(map = TRUE),
          structured = list(reduce = TRUE))

#group counts by year and find totals
year.totals = 
from.dfs(
  mapreduce(input = filter.norm.data,
            map = function(k, data) {
              keyval(data$year + sample(seq(from=0,to=.9, by = .01),1), data$count)},
            reduce = function(year, counts) keyval(floor(year), sum(unlist(counts), na.rm = TRUE)),
            combine = T),
  to.data.frame = T)
names(year.totals) = c("year", "count")
yt = tapply(year.totals$count, year.totals$year, function(x) sum(x))
year.totals = data.frame(year = as.integer(names(yt)), count = yt)
year.totals = year.totals[-(1:77),]



totals = year.totals$count[order(year.totals$year)]

#group by ngram and compute p-values
reduce.pval = function(ngram, count.sparse) {
  names(count.sparse) = c("year", "count")
  count.sparse = subset(count.sparse, year >= 1693)
  count = rep(min(count.sparse$count) - 1, length(totals))
  count[count.sparse$year - min(year.totals$year) + 1] = count.sparse$count
  p.value = sapply(2:length(count), 
                   function(i) {
                     r = (i - 1):i
                     if(all(count[r] == 0)) 1 
                     else prop.test(count[r], totals[r], alternative = "less")$p.value})
  if (min(p.value) < 10^(-5)) keyval(ngram, list(count = count, p.value = p.value))}

pvals = 
  mapreduce(input = filter.norm.data,
            map = function(k,v) {
              keyval(v$ngram, v[c('year','count')])},
            reduce = reduce.pval,
            reduce.on.data.frame = T)

pvals.by.year = mapreduce(pvals, 
                          "/user/antonio/out",
                          map = function(ng, v) keyval(which(rank(v$p.value, ties.method = "first") == 1), 
                                                       list(p.value = v$p.value, count = v$count, ngram = ng)), 
                          reduce = function(year, vv) keyval(year,
                                                             vv[rank(sapply(vv, 
                                                                            function(x) x$p.value[[year]]), ties.method = "first") <= 200])) 
                                                                                                                   
#visualization

fd = data.frame(id = paste("a", 1:100, sep = ""), time = as.integer((0:999)/100), x = cumsum(rnorm(1000)), y = cumsum(rnorm(1000)),  size = .01)
M <- gvisMotionChart(fd, options = list(height = 1000, width = 2000)); cat(M$html$chart, file="~/Desktop/tmp.html")