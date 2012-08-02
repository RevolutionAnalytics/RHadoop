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

tsv.data = to.dfs(data.frame(x = 1:100, y = rnorm(100), z = runif(100), w = 1:100), format = make.output.format("csv", sep = "\t"))
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
  output = tempfile(),
  output.format = csv.format,
  map = function(k,v){
    # complicated function here
    keyval(k,v)},
  reduce = function(k, vv) {
    #complicated function here
    keyval(k, vv[[1]])})
## @knitr end
## @knitr getting-data.create.fields.list
qw = function(...) as.character(match.call())[-1]
fields <- qw(mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb) 
field.size = 8
## @knitr end
## @knitr getting-data.fwf.reader
fwf.reader <- function(con, nrecs) {  
  lines <- readLines(con, nrecs)  
  if (length(lines) == 0) {
    NULL}
  else {
    df =
      as.data.frame(
        matrix(
          sapply(
            split(unlist(strsplit(lines, "")), 
                  ceiling(1:2816/8)), 
            paste, collapse = ""), 
          ncol=length(fields), byrow=T))
    names(df) = fields
    keyval(NULL, df)}} 
## @knitr end
## @knitr getting-data.fwf.writer
fwf.writer <- function(k, v, con, vectorized) {
  ser <- function(df) paste(apply(df, 1, function(x) paste(format(x, width = field.size), collapse = "")), collapse = "\n")
  out <- if(vectorized) {
    ser(do.call(rbind,v))}
  else {
    ser(v)}
  writeLines(out, con = con)}
## @knitr end
## @knitr getting-data.generate.fwf.data
fwf.data <- to.dfs(mtcars, format = make.output.format(mode = "text", format = fwf.writer))
## @knitr end
## @knitr getting-data.from.dfs.one.line
out <- from.dfs(mapreduce(input = fwf.data,
                          input.format = make.input.format(mode = "text", format = fwf.reader)), structured = T)
out$val
## @knitr end
## @knitr getting-data.from.dfs.multiple.lines
out <- from.dfs(mapreduce(input = fwf.data,
                          input.format = make.input.format(mode = "text", format = fwf.reader),
                          vectorized = list(map = TRUE)))
## @knitr end
## @knitr getting-data.cyl.frequency.count
out <- from.dfs(mapreduce(input = fwf.data,
                          input.format = make.input.format(mode = "text", format = fwf.reader),
                          map = function(key, value) keyval(value[,"cyl"], 1),
                          reduce = function(key, value) keyval(key, sum(unlist(value))),
                          combine = TRUE), 
                structured = TRUE)
df <- data.frame(out$key, out$val)
names(df) <- c("cyl","count")
df
## @knitr end
## @knitr getting-data.cyl.vectorized.frequency.count
out <- from.dfs(mapreduce(input = fwf.data,
                          input.format = make.input.format(mode = "text", format = fwf.reader),
                          map = function(key, value) keyval(value[,"cyl"], 1, vectorized = TRUE),
                          reduce = function(key, value) keyval(key, sum(unlist(value))),
                          combine = TRUE,
                          vectorized = list(map = TRUE),
                          structured = list(map = TRUE)), structured = TRUE)
df <- data.frame(out$key, out$val)
names(df) <- c("cyl","count")
df
## @knitr end
