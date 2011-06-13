#! /usr/bin/env Rscript
options(warn=-1)

library(rjson)

createReader = function(N=2000, textinputformat){
  con = file("stdin", open="r")
  close = function(){
    ## close(con)
  }
  readChunk = function(n=N){
    lines = readLines(con, n = n, warn=FALSE)
    if(length(lines) > 0){
      return(textinputformat(lines))
    }else{
      return(NULL) 
    }
  }
  return(list(close = close, get  = readChunk))
}

sendKV = function(k, v){
  ## f is data frame, keys, values
  lapply(1:length(k),function(r){
    cat(TEXTOUTPUTFORMAT(k,v))
  })
}

send = function(r){
  cat(TEXTOUTPUTFORMAT(r[1], r[2]))
}

counter = function(group="r-stream",family, value){
  cat(sprintf("report:counter:%s,$s,%s",
              as.character(group),
              as.character(family),
              as.integer(value)),
      stderr())
}

status = function(what){
  cat(sprintf("report:status:%s",
              as.character(what)),
      stderr())
}

Sum = function(id,p=1){
  if(typeof(p)=='double')
    cat(sprintf("DoubleValueSum:%s\t%s\n",id,p))
  else
    cat(sprintf("LongValueSum:%s\t%s\n",id,p))
}
Min = function(id,p){
  if(typeof(p)=='double')
    cat(sprintf("DoubleValueMin:%s\t%s\n",id,p))
  else
    cat(sprintf("LongValueMin:%s\t%s\n",id,p))
}
Max = function(id,p){
  if(typeof(p)=='double')
    cat(sprintf("DoubleValueMax:%s\t%s\n",id,p))
  else
    cat(sprintf("LongValueMax:%s\t%s\n",id,p))
}
Hist = function(id,p){
  ## No. of Uniq, Min,Median,Max,Average,Std Dev
    cat(sprintf("ValueHistogram:%s\t%s\n",id,p))
}  

driverFunction = function(MAPRED, N, TEXTINPUTFROMAT, TEXTOUTPUTFORMAT){
  k = createReader(N, TEXTINPUTFORMAT)
  while( !is.null(d <- k$get())){
    MAPRED(d)
  }
  k$close()
  invisible()
}
  


