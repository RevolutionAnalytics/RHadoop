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
my.data <- list(TRUE, list("nested list", 7.2), seq(1:3), letters[1:4], matrix(1:25, nrow = 5,ncol = 5))
## @knitr end
## @knitr getting-data.to.dfs
hdfs.data <- to.dfs(my.data)
## @knitr end
## @knitr getting-data.object.length.frequency
result <- mapreduce(input = hdfs.data,
  map = function(k,v) keyval(length(v), 1),
  reduce = function(k,vv) keyval(k, sum(unlist(vv))))

from.dfs(result)
## @knitr end
## @knitr getting-data.tsv.reader
myTSVReader <- function(line){
  delim <- strsplit(line, split = "\t")[[1]]
  keyval(delim[[1]], delim[-1])} # first column is the key, note that column indexes moved by 1
## @knitr end
## @knitr getting-data.frequency.count
mrResult <- mapreduce(input = hdfsData,
                      textinputformat = myTSVReader,
                      map = function(k,v) keyval(v[[1]], 1),
                      reduce = function(k,vv) sapply(vv, sum(unlist(vv)))
## @knitr end
## @knitr getting-data.named.columns
mySpecificTSVReader <- function(line){
  delim <- strsplit(line, split = "\t")[[1]]
  keyval(delim[[1]], list(location = delim[[2]], name = delim[[3]], value = delim[[4]]))}
## @knitr end
## @knitr getting-data.named.column.access
                      mrResult <- mapreduce(input = hdfsData,
                                            textinputformat = mySpecificTSVReader,
                                            map = function(k, v) { 
                                              if (v$name == "blarg"){
                                                keyval(k, log(v$value))
                                              }
                                            },
                                            reduce = function(k, vv) keyval(k, mean(unlist(vv))))                      
## @knitr end
## @knitr getting-data.csv.output
myCSVOutput <- function(k, v){
  keyval(paste(k, paste(v, collapse = ","), sep = ","))}
## @knitr end
## @knitr getting-data.csv.output.simpler
myCSVOutput = csvtextoutputformat(sep = ",")
## @knitr end
## @knitr getting-data.explicit.output.arg
mapreduce(input = hdfsData,
          output = "/rhadoop/output/",
          textoutputformat = myCSVOutput,
          map = function(k,v){
            # complicated function here
          },
          reduce = function(k,v) {
            #complicated function here
          })
## @knitr end
## @knitr getting-data.save.output
hdfs.get("/rhadoop/output/", "/home/rhadoop/filesystemoutput/")
## @knitr end                      
