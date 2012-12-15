# Copyright 2011 Revolution Analytics
#    
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Start cluster
# $WHIRR_HOME/bin/whirr  launch-cluster --config ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/hadoop-ec2-lzo.properties 2>&1 
# $WHIRR_HOME/bin/whirr  run-script --script ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/rmr-1.3.sh  --config ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/hadoop-ec2-lzo.properties
# $WHIRR_HOME/bin/whirr  run-script --script ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/lzo.sh  --config ~/Projects/Revolution/RHadoop/rmr/pkg/tools/whirr/hadoop-ec2-lzo.properties

library(rmr2)
library(robustbase)
#using 2.0.2
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
# rmr.options(backend = "local")

# real data
# do  distcp and then scatter
# hadoop distcp s3n://$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY@datasets.elasticmapreduce/ngrams/books/20090715/eng-all/1gram/ hdfs:///user/antonio/

#source = "hdfs:///user/antonio/1gram/data"
rmr.options(backend = "hadoop")

ngram.format = function(lines){
  data = 
    as.data.frame(
      do.call(rbind, strsplit(lines, "\t"))[,1:3])
  names(data) = c("ngram", "year", "count")
  data$year = as.integer(as.character(data$year))
  data$count = as.integer(as.character(data$count))
  data}

#filter and normalize n-grams
filter.map = function(k, lines) {
  data = ngram.format(lines)
  data$ngram = tolower(data$ngram)
  data = 
    data[
      regexpr(
        "^[a-z+'-]+$", 
        data$ngram) > 0,]
  keyval(
    data$ngram[data$year > 1700], 
    data[data$year > 1700, -1])}

filter.reduce = 
  function(ngram, year.counts) {
    names(year.counts) = c("year", "counts")
    keyval(
      ngram, 
      aggregate(
        year.counts$counts, 
        by = list(year.counts$year), sum))}

filter.norm.data = 
  mapreduce(input = source,
            input.format = "text", #comment this on real data
            map = filter.map,
            reduce = filter.reduce)
  
from.dfs(rmr.sample(filter.norm.data, method="any", n = 50))

totals.map = 
  function(ngram, year.counts) {
    names(year.counts) = c("year", "counts")
    keyval(
      year.counts$year + 
        sample(seq(from=0,to=.9, by = .01), 
               nrow(year.counts), replace = TRUE), 
      year.counts$counts)}

totals.reduce = 
  function(year, counts) keyval(floor(year), sum(counts, na.rm = TRUE))
  
#group counts by year and find totals
year.totals = 
from.dfs(
  mapreduce(input = filter.norm.data,
            map = totals.map,
            reduce = totals.reduce,
            combine = TRUE))

names(year.totals) = c("year", "count")
year.totals = tapply(year.totals$count, year.totals$year, function(x) sum(x))

#year.totals = year.totals[-(1:77)]

#group by ngram and compute p-values

outlier.ngram =
  function(ngram, count.sparse) {
    years = as.numeric(names(year.totals))
    rownames(count.sparse) = count.sparse$year
    count = 
      rep(min(count.sparse$count) - 1, length(year.totals))
    count[count.sparse$year - min(years) + 1] = 
      count.sparse$count
    log.freq = log((count + 1)/(year.totals + 1))
    keyval(
      years[-1] + 
        sample(
          seq(from = 0, to =.9, by = .1), 
          length(year.totals) - 1, replace = TRUE), 
      data.frame(ngram, log.freq[-length(log.freq)], log.freq[-1], stringsAsFactors = FALSE))}

outlier.map = 
  function(ngrams, count.sparse) {
    names(count.sparse) = c("year", "count")
    count.sparse = split(count.sparse, ngrams)
    c.keyval(lapply(unique(ngrams), function(ng) outlier.ngram(ng, count.sparse[[ng]])))}

outlier.reduce = function(year, log.freqs) {
  #rmr.str(year)
  #rmr.str(log.freqs)
  keyval(NULL, unique(log.freqs[!adjOutlyingness(log.freqs[,2:3])$nonOut,1]))}

outlier.ngrams = c()
outlier.ngrams[
  unique(
    unlist(
      values(
        from.dfs(
          mapreduce(
            input = filter.norm.data,
            map = outlier.map,
            reduce = outlier.reduce)))))] = TRUE

ngram.filter = 
  function(ngram) !is.na(outlier.ngrams[ngram])
  

graph.data = 
  from.dfs(
    mapreduce(
      input = filter.norm.data,
      map = function(ngram, data) if(ngram.filter(ngram)) keyval(ngram, data)))
#visualization

library(googleVis)
graph.data.frame = cbind(keys(graph.data), values(graph.data))
graph.data.frame = cbind(graph.data.frame[-nrow(graph.data.frame),],  graph.data.frame[-1,])
graph.data.frame = graph.data.frame[graph.data.frame[1:10,1] == graph.data.frame[1:10,4],c(1,2,3,5,6)]

names(graph.data.frame) = rmr2:::qw(id,time,count, time.1, count.1)
graph.data.frame$average = 
  as.vector(
    with(
      graph.data.frame, 
      sqrt((count/year.totals[as.character(time)]) * 
             (count.1/year.totals[as.character(time.1)]))))
graph.data.frame$difference = 
  as.vector(
    with(
      graph.data.frame, 
      (count/year.totals[as.character(time)]) / 
        (count.1/year.totals[as.character(time.1)])))
plot(gvisMotionChart(graph.data.frame[,c("id","time","average","difference")], options = list(height = 1000, width = 2000)))