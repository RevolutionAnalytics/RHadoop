stopifnot(require(RevoHDFS, quietly=TRUE))
stopifnot(require(RUnit, quietly=TRUE))

test.hdfs.read <- function()
{
        ## Load airline test data and put it into the Hadoop file system
        localData <- system.file(file.path("unitTestData", "AirlineDemo1kNoMissing.csv"), package="RevoHDFS")
        hdfs.mkdir("/test/airline")
        hdfs.put(localData, "/test/airline/AirlineDemo1kNoMissing.csv")

	## Test checks
	file1 <- hdfs.file("/test/airline/AirlineDemo1kNoMissing.csv","r")
	checkTrue(is.character(somedata <- rawToChar(hdfs.read(file1,100))))
	hdfs.close(file1)

	##Breakdown
        hdfs.delete("/test")
}

