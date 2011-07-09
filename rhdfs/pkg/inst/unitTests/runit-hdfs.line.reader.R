stopifnot(require(RevoHDFS, quietly=TRUE))
stopifnot(require(RUnit, quietly=TRUE))

test.hdfs.line.reader <- function()
{
        ## Load airline test data and put it into the Hadoop file system
        localData <- system.file(file.path("unitTestData", "AirlineDemo1kNoMissing.csv"), package="RevoHDFS")
        hdfs.mkdir("/test/airline")
        hdfs.put(localData, "/test/airline/AirlineDemo1kNoMissing.csv")

	## Test checks
	m <- hdfs.line.reader("/test/airline/AirlineDemo1kNoMissing.csv", n=100)
	x <- m$read()
	checkTrue(x[3] == "-3,18.583334,\"Monday\"")
        checkTrue(x[50] == "-7,12.166667,\"Monday\"")
	checkTrue(x[99] == "0,18.750002,\"Monday\"")
	m$close()
	
	##Breakdown
	rm(x)
        hdfs.delete("/test")
}

