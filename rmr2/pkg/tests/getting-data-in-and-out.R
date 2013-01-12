library(rmr2)
## @knitr getting-data.IO.formats
rmr2:::IO.formats
## @knitr getting-data.make.input.format.csv
make.input.format("csv")
## @knitr getting-data.make.output.format.csv
make.output.format("csv")
## @knitr getting-data.generic.list
my.data = list(TRUE, list("nested list", 7.2), seq(1:3), letters[1:4], matrix(1:25, nrow = 5,ncol = 5))
## @knitr getting-data.to.dfs
hdfs.data = to.dfs(my.data)
## @knitr getting-data.object.length.frequency
result = mapreduce(
  input = hdfs.data,
  map = function(k, v) keyval(lapply(v, length), 1),
  reduce = function(k, vv) keyval(k, sum(vv)))

from.dfs(result)
## @knitr end
## @knitr getting-data.tsv.reader
tsv.reader = function(con, nrecs){
  lines = readLines(con, 1)
  if(length(lines) == 0)
    NULL
  else {
    delim = strsplit(lines, split = "\t")
    keyval(
      sapply(delim, 
             function(x) x[1]), 
      sapply(delim, 
             function(x) x[-1]))}} 
## first column is the key, note that column indexes moved by 1
## @knitr getting-data.tsv.input.format
tsv.format = 
  make.input.format(
    format = tsv.reader,
    mode = "text")
## @knitr getting-data.generate.tsv.data

tsv.data = 
  to.dfs(
    data.frame(
      x = 1:100, y = rnorm(100), 
      z = runif(100), 
      w = 1:100), 
    format = 
      make.output.format("csv", sep = "\t"))
## @knitr getting-data.frequency.count
freq.counts = 
  mapreduce(
    input = tsv.data,
    input.format = tsv.format,
    map = function(k, v) keyval(v[1,], 1),
    reduce = function(k, vv) keyval(k, sum(vv)))
## @knitr getting-data.named.columns
tsv.reader = 
  function(con, nrecs){
    lines = readLines(con, 1)
    if(length(lines) == 0)
      NULL
    else {
      delim = strsplit(lines, split = "\t")
      keyval(
        sapply(delim, function(x) x[1]), 
        data.frame(
          location = sapply(delim, function(x) x[2]),
          name = sapply(delim, function(x) x[3]),
          value = sapply(delim, function(x) x[4])))}}

## @knitr getting-data.tsv.input.format.1
tsv.format = 
  make.input.format(
    format = tsv.reader,
    mode = "text")
## @knitr getting-data.named.column.access
freq.counts =
  mapreduce(
    input = tsv.data,
    input.format = tsv.format,
    map = 
      function(k, v) { 
        filter = (v$name == "blarg")
        keyval(k[filter], log(as.numeric(v$value[filter])))},
    reduce = function(k, vv) keyval(k, mean(vv)))                      
## @knitr getting-data.csv.output
csv.writer = function(kv, con){
  cat(
    paste(
      apply(cbind(1:32, mtcars), 
            1, 
            paste, collapse = ","), 
      collapse = "\n"),
    file = con)}
## @knitr getting-data.csv.output.simpler
csv.format = make.output.format("csv", sep = ",")
## @knitr getting-data.explicit.output.arg
mapreduce(
  input = hdfs.data,
  output = tempfile(),
  output.format = csv.format,
  map = function(k, v){
    # complicated function here
    keyval(1, v)},
  reduce = function(k, vv) {
    #complicated function here
    keyval(k, vv[[1]])})
## @knitr getting-data.create.fields.list
fields <- rmr2:::qw(mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb) 
field.size = 8
## @knitr getting-data.fwf.reader
fwf.reader <- function(con, nrecs) {  
  lines <- readLines(con, nrecs)  
  if (length(lines) == 0) {
    NULL}
  else {
    split.lines = unlist(strsplit(lines, ""))
    df =
      as.data.frame(
        matrix(
          sapply(
            split(
              split.lines, 
              ceiling(1:length(split.lines)/field.size)), 
            paste, collapse = ""), 
          ncol=length(fields), byrow=T))
    names(df) = fields
    keyval(NULL, df)}} 
fwf.input.format = make.input.format(mode = "text", format = fwf.reader)
## @knitr getting-data.fwf.writer
fwf.writer <- function(kv, con, keyval.size) {
  ser =
    function(df) 
      paste(
          apply(df,
                1, 
                function(x) 
                  paste(
                    format(
                      x, 
                      width = field.size), 
                    collapse = "")), 
        collapse = "\n")
  out = ser(values(kv))
  writeLines(out, con = con)}
fwf.output.format = make.output.format(mode = "text", format = fwf.writer)
## @knitr getting-data.generate.fwf.data
fwf.data <- to.dfs(mtcars, format = fwf.output.format)
## @knitr getting-data.from.dfs.one.line
out <- from.dfs(mapreduce(input = fwf.data,
                          input.format = fwf.input.format))
out$val
## @knitr getting-data.cyl.frequency.count
out <- from.dfs(mapreduce(input = fwf.data,
                          input.format = fwf.input.format,
                          map = function(key, value) keyval(value[,"cyl"], 1),
                          reduce = function(key, value) keyval(key, sum(unlist(value))),
                          combine = TRUE))
df <- data.frame(out$key, out$val)
names(df) <- c("cyl","count")
df
## @knitr end

