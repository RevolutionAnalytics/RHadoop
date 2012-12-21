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
## @knitr fake-data
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
  
source = "/tmp/fake-ngram-data"
# rmr.options(backend = "local")

## @knitr distcp
# do  distcp and then scatter
# hadoop distcp -m 100 s3n://$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY@datasets.elasticmapreduce/ngrams/books/20090715/eng-all/1gram/ hdfs:///user/antonio/

#source = scatter("hdfs:///user/antonio/1gram/data")
rmr.options(backend = "hadoop")
rmr.option(keyval.length = 10^5)

## @knitr ngram.format
ngram.format = function(lines){
  data = 
    as.data.frame(
      do.call(rbind, strsplit(lines, "\t"))[,1:3])
  names(data) = c("ngram", "year", "count")
  data$year = as.integer(as.character(data$year))
  data$count = as.integer(as.character(data$count))
  data}

## @knitr filter.map 
filter.map = function(., lines) {
  ngram.data = ngram.format(lines)
  ngram.data[
    regexpr(
      "^[A-za-z+'-]+$", 
      ngram.data$ngram) > -1 & 
      ngram.data$year > 1800,]}

## @knitr filtered.data
filtered.data = 
  mapreduce(input = source,
            input.format = "text", #comment this on real data
            map = filter.map)
## @knitr sample-data
from.dfs(rmr.sample(filtered.data, method="any", n = 50))


## @knitr totals.map
totals.map = 
  function(., ngram.data) {
    total = tapply(ngram.data$count, ngram.data$year, sum)
    keyval(names(total), as.vector(total))}

## @knitr totals.reduce
totals.reduce = 
  function(year, count) 
    keyval(year, sum(count, na.rm = TRUE))
  
## @knitr year.totals
year.totals.kv = 
  from.dfs(
    mapreduce(input = filtered.data,
              map = totals.map,
              reduce = totals.reduce,
              combine = TRUE))

## @knitr year.totals-finish
year.totals = c()
year.totals[keys(year.totals)] = values(year.totals)

## @knitr outlier.map
outlier.map = 
  function(., ngram.data) {
    k = ngram.data$year + cksum(ngram.data$ngram)%%10/10
    c.keyval(
      keyval(k, ngram.data),
      keyval(k + 1, ngram.data))}

## @knitr outlier.reduce
library(robustbase)
outlier.reduce =
  function(., ngram.data) {
    years = range(ngram.data$year)
    if(years[1] == years[2])
      NULL
    else {
      ngram.data = dcast(ngram.data, ngram ~ year, fill = 0)
      filter = 
        !adjOutlyingness(
          log(
            t(
              t(ngram.data[,2:3] + 1)/
                as.vector(year.totals[as.character(years)] + 1)))
          )$nonOut
      as.character(ngram.data[filter,'ngram'])}}

## @knitr outlier.ngram
outlier.ngram = c()
outlier.ngram[
  unique(
    values(
      from.dfs(
        mapreduce(
          input = filtered.data,
          map = outlier.map,
          reduce = outlier.reduce))))] = TRUE

## @knitr ngram.filter
ngram.filter = 
  function(ngram) !is.na(outlier.ngram[as.character(ngram)])  

## @knitr plot.data
plot.data = 
  values(
    from.dfs(
      mapreduce(
        input = filtered.data,
        map = function(., ngram.data) 
          ngram.data[ngram.filter(ngram.data$ngram),])))

## @knitr plot.data.frame
plot.data = 
  cbind(
    plot.data[-nrow(plot.data),],  
    plot.data[-1,])
plot.data = 
  plot.data[
    plot.data[,1] == plot.data[,4],
    c(1,2,3,5,6)]
names(plot.data) = 
  c("id","time","count", "time.1", "count.1")
plot.data$average = 
  as.vector(
    with(
      plot.data, 
      sqrt((count/year.totals[as.character(time)]) * 
             (count.1/year.totals[as.character(time.1)]))))
plot.data$ratio = 
  as.vector(
    with(
      plot.data, 
      (count/year.totals[as.character(time)]) / 
        (count.1/year.totals[as.character(time.1)])))

## @knitr plot
library(googleVis)
plot(
  gvisMotionChart(
    plot.data[,c("id","time","average","difference")], 
    options = list(height = 1000, width = 2000)))