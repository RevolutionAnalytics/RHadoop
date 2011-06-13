stopifnot(require(RevoHDFS, quietly=TRUE))
stopifnot(require(RUnit, quietly=TRUE))

test.hdfs.read.text.file <- function()
{
        ## Load airline test data and put it into the Hadoop file system
        localData <- system.file(file.path("unitTestData", "AirlineDemo1kNoMissing.csv"), package="RevoHDFS")
        hdfs.mkdir("/test/airline")
        hdfs.put(localData, "/test/airline/AirlineDemo1kNoMissing.csv")

	## Test checks
	x <- hdfs.read.text.file("/test/airline/AirlineDemo1kNoMissing.csv")
	checkTrue(x[3] == "-3,18.583334,\"Monday\"")
        checkTrue(x[645] == "-1,12.583334,\"Tuesday\"")
	checkTrue(x[984] == "-1,15.000000,\"Tuesday\"")
	
	##Breakdown
	rm(x)
        hdfs.delete("/test")
}

