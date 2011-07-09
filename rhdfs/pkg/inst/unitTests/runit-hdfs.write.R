stopifnot(require(RevoHDFS, quietly=TRUE))
stopifnot(require(RUnit, quietly=TRUE))

test.hdfs.write <- function()
{
        ## Load airline test data and put it into the Hadoop file system
        localData <- system.file(file.path("unitTestData", "AirlineDemo1kNoMissing.csv"), package="RevoHDFS")
	hdfs.mkdir("/test/airline")
        hdfs.put(localData, "/test/airline/AirlineDemo1kNoMissing.csv")
        ## read and write testing
	file1 <- hdfs.file("/test/airline/AirlineDemo1kNoMissing.csv","r")
	checkTrue(is.character(somedata <- rawToChar(hdfs.read(file1,100))))
        w.file <- hdfs.file("/test/airline/tmp/sample.csv","w")
	N <- 1000
	checkTrue(is.character(somedata <- rawToChar(hdfs.read(file1,N))))
	checkTrue(hdfs.write(somedata,w.file))
	checkTrue(hdfs.close(w.file))
	w.file <- hdfs.file("/test/airline/tmp/sample.csv","r")
	x <- c()
	while(TRUE){
  		if(is.null(a <- hdfs.read(w.file,N))) break
  		x <- c(x,a)
	}
	hdfs.close(w.file)
	checkTrue(all(somedata== unserialize(x)))

	## Breakdown
	hdfs.delete("/test")
}
