library(rmr)
## @knitr getting-data.IO.formats
rmr:::IO.formats
## @knitr end
## @knitr getting-data.make.input.format.csv
make.input.format("csv")
## @knitr end
## @knitr getting-data.make.output.format.csv
make.output.format("csv")
## @knitr end
## @knitr getting-data.getting.data.generic.list
my.data = list(TRUE, list("nested list", 7.2), seq(1:3), letters[1:4], matrix(1:25, nrow = 5,ncol = 5))
## @knitr end
## @knitr getting-data.to.dfs
hdfs.data = to.dfs(my.data)
## @knitr end
## @knitr getting-data.object.length.frequency
result = mapreduce(
  input = hdfs.data,
  map = function(k,v) keyval(length(v), 1),
  reduce = function(k,vv) keyval(k, sum(unlist(vv))))

from.dfs(result)
## @knitr end
## @knitr getting-data.tsv.reader
tsv.reader = function(con, nrecs){
  lines = readLines(con, 1)
  if(length(lines) == 0)
    NULL
  else {
    delim = strsplit(lines, split = "\t")[[1]]
    keyval(delim[1], delim[-1])}} # first column is the key, note that column indexes moved by 1
## @knitr end
## @knitr getting-data.tsv.input.format
tsv.format = 
  make.input.format(
    format = tsv.reader,
    mode = "text")
## @knitr end
## @knitr getting-data.generate.tsv.data
library(car)
tsv.data = to.dfs(Moore, format = make.output.format("csv", sep = "\t"))
## @knitr getting-data.frequency.count
freq.counts = 
  mapreduce(
    input = tsv.data,
    input.format = tsv.format,
    map = function(k,v) keyval(v[[1]], 1),
    reduce = function(k,vv) keyval(k, sum(unlist(vv))))
## @knitr end
## @knitr getting-data.named.columns
tsv.reader = 
  function(con, nrecs){
    lines = readLines(con, 1)
    if(length(lines) == 0)
      NULL
    else {
      delim = strsplit(lines, split = "\t")[[1]]
      keyval(delim[[1]], list(location = delim[[2]], name = delim[[3]], value = delim[[4]]))}}
## @knitr end
## @knitr getting-data.tsv.input.format.1
tsv.format = 
  make.input.format(
    format=tsv.reader,
    mode = "text")
## @knitr end
## @knitr getting-data.named.column.access
freq.counts =
  mapreduce(
    input = tsv.data,
    input.format = tsv.format,
    map = 
      function(k, v) { 
        if (v$name == "blarg"){
          keyval(k, log(v$value))}},
    reduce = function(k, vv) keyval(k, mean(unlist(vv))))                      
## @knitr end
## @knitr getting-data.csv.output
csv.writer = function(k, v){
  paste(k, paste(v, collapse = ","), sep = ",")}
## @knitr end
## @knitr getting-data.csv.output.simpler
csv.format = make.output.format("csv", sep = ",")
## @knitr end
## @knitr getting-data.explicit.output.arg
mapreduce(
  input = hdfs.data,
  output = "/tmp/rhadoop/output/",
  output.format = csv.format,
  map = function(k,v){
    # complicated function here
    keyval(k,v)},
  reduce = function(k, vv) {
    #complicated function here
    keyval(k, vv[[1]])})
## @knitr end
                   