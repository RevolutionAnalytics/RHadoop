reader.for.hdfs <- function(filelocation, batchsize,F,discard.header=FALSE,buffer=as.integer(5*1024^2)){
  ## takes only a single file
  handle <- hdfs.line.reader(filelocation,n=batchsize,buffer=buffer)
  if(discard.header) handle$read(1)
  f <- function(reset){
    if(reset){
      ## When reset=TRUE it indicates that the data should be reread from the
      ## beginning by subsequent calls. The chunks need not be the same size or
      ## in the same order when the data are reread, but the same data must be
      ## provided in total
      handle<<-hdfs.line.reader(filelocation,n=batchsize,buffer=buffer)
      if(discard.header) handle$read(1)
      cat(sprintf("[%s] Opened %s from the beginning\n", date(),filelocation))
      NULL
    }else{
      ## When this argument is FALSE it returns a data frame with the
      ## next chunk of data or NULL if no more data are available
      r <- handle$read()
      if(length(r)==0)
        return(NULL)
      else
        return(F(r))
    }
  }
  return(f)
}

airlineparse <- function(stringdata){
  ## ArrDelay  ~ DayOfWeek
  ## Following simple way to find out the columns
  ## y <- strsplit(hdfs.line.reader("/airline/data/1987.csv",n=1)$read(),",")[[1]]
  ## which(y %in% "ArrDelay")(4)
  ## which(y %in% "DayOfWeek")(15)
  x <- lapply(strsplit(stringdata,","),function(r) r[c(4,15)])
  arrdelay <- as.numeric(unlist(lapply(x,"[[",2)))
  day.of.week <- factor(unlist(lapply(x,"[[",1)),
                        levels=1:7,
                        labels=c("Mon","Tues","Wed","Thurs","Fri","Sat","Sun"))
  f <- data.frame(ArrDelay = arrdelay, DayOfWeek = day.of.week)
  ## remove NA's
  f[!is.na(f$ArrDelay) & !is.na(f$DayOfWeek),]
}


library(RevoHDFS)
library(biglm)
hdfs.init()

p <- reader.for.hdfs("/airline/data/1987.csv",
                     batchsize=5000,buff=as.integer(10*1024^2),
                     F=airlineparse,discard.header=TRUE)
tm <- system.time({
  a <- bigglm(ArrDelay~DayOfWeek,data=p,maxit=3)
})
