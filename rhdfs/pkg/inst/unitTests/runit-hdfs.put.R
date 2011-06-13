stopifnot(require(RevoHDFS, quietly=TRUE))
stopifnot(require(RUnit, quietly=TRUE))

test.hdfs.put <- function()
{
        ## Load airline test data and put it into the Hadoop file system
        localData <- system.file(file.path("unitTestData", "AirlineDemo1kNoMissing.csv"), package="RevoHDFS")
        hdfs.mkdir("/test/airline")
        checkTrue(hdfs.put(localData, "/test/airline/AirlineDemo1kNoMissing.csv"))
	checkTrue(hdfs.file.info("/test/airline/AirlineDemo1kNoMissing.csv")$size==23627)
	
        ##Breakdown
        hdfs.delete("/test")
}

