#! /usr/bin/env Rscript
options(warn=-1)


createReader <- function(colspec=NULL,colsep = "[[:space:]]+",N=2000){
  ## colspec is a list of the form
  ## k = types of "character"|"integer"|"numeric"|"logical"| where k is a column number
  ## only those columns corresponding to values of k will be returned
  ## if null, it is assumed all columns will be used and all are strings
  ## it will just split the lines for you and return a list of character vectors
  ## and a vector of all will be returned
  ## if typeof(colspec)  == "string", a vector of strings,
  ## if typeof(colespec) == integer, a vector of integers
  ## the difference between list and character, is that the former
  ## returns a data frame
  ## colsep is used to split fields
  ## quote strings with space will not be respected.
  GetType <- function(f){
    if(is.character(f)){
      if(f %in% c("character","integer","numeric","logical")) return(f)
      else return("character")
    }else 
    return(typeof(f))
  }
  typeConverter <- c("character"=as.character,"integer" = as.integer,
                     "numeric" = as.numeric, "logical" = as.logical)
  if(!is.null(colspec)){
    colspec <- as.list(colspec)
    if(is.list(colspec)){
      if(is.null(names(colspec))) names(colspec) <- 1:length(colspec)
      whichcols <- as.integer(names(colspec))
      if(length(whichcols)==0) whichcols <- 1:length(colspec)
      cols.types <- unlist(lapply(colspec,GetType))
      cols.conv <- typeConverter[cols.types]
    }else {
      cols.conv <- typeConverter[[GetType(colspec)]]
    }
  }
  con <- file("stdin",open="r")
  close <- function(){
    ## close(con)
  }
  readChunk <- function(n=N){
    lines <- readLines(con, n = n, warn=FALSE)
    if(length(lines) > 0){
      if(is.null(colspec)){
        y <- strsplit(lines, colsep)
      }else{
        if(is.list(colspec)){
          z <- strsplit(lines, colsep)
          colspec.names <- as.numeric(names(colspec))
          y <- data.frame(lapply(1:length(cols.conv), function(i)
                                 {
                                   cols.conv[[i]]( unlist(lapply(z,"[[",colspec.names[i])) )
                                 }
                                 ),stringsAsFactors=FALSE)
          colnames(y) <- 1:ncol(y)
        }else{
          ## we have a a scalar type
          y <- cols.conv(unlist(strsplit(lines, colsep)))
        }
      }
      return(y)
    }else{
      return(NULL) 
    }
  }
  return(list(close = close, get  = readChunk))
}

sendKV <- function(k,v){
  ## f is data frame, keys, values
  lapply(1:length(k),function(r){
    cat(k[r],"\t",v[r],"\n",sep="",file="")
  })
}
send <- function(r){
  cat(r[1],"\t",r[2],"\n",sep="",file="")
}

counter <- function(group="r-stream",family, value){
  cat(sprintf("report:counter:%s,$s,%s", as.character(group),as.character(family),as.integer(value)),stderr())
}
status <- function(what){
  cat(sprintf("report:status:%s",as.character(what)), stderr())
}

R2Text <- function(o,s=function(r) rawToChar(serialize(r,NULL,ascii=TRUE))) s(o)
Vec2Text <- function(r,collapse=" ") paste(r,collapse=collapse)
Sum <- function(id,p=1){
  if(typeof(p)=='double')
    cat(sprintf("DoubleValueSum:%s\t%s\n",id,p))
  else
    cat(sprintf("LongValueSum:%s\t%s\n",id,p))
}
Min <- function(id,p){
  if(typeof(p)=='double')
    cat(sprintf("DoubleValueMin:%s\t%s\n",id,p))
  else
    cat(sprintf("LongValueMin:%s\t%s\n",id,p))
}
Max <- function(id,p){
  if(typeof(p)=='double')
    cat(sprintf("DoubleValueMax:%s\t%s\n",id,p))
  else
    cat(sprintf("LongValueMax:%s\t%s\n",id,p))
}
Hist <- function(id,p){
  ## No. of Uniq, Min,Median,Max,Average,Std Dev
    cat(sprintf("ValueHistogram:%s\t%s\n",id,p))
}  

driverMAPFunction <- function(MAP,colspec,colsep,N){
  k <- createReader(colspec,colsep,N)
  while( !is.null(d <- k$get())){
    MAP(d)
  }
  k$close()
  invisible()
}

driverREDUCEFunction <- function(REDUCE,N){
  con <- file("stdin",open="r")
  while(!is.null(lines <- readLines(con, n = N, warn=FALSE))){
    x <- unlist(strsplit(lines,"\t"))
    dim(x) <- c(length(x)/2,2)
    REDUCE(x)
  }
}

## driverFunction(MAP)
  


