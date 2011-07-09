stopifnot(require(RevoHDFS, quietly=TRUE))
stopifnot(require(RUnit, quietly=TRUE))

test.hdfs.exists <- function()
{
        ## Load airline test data and put it into the Hadoop file system
        localData <- system.file(file.path("unitTestData", "AirlineDemo1kNoMissing.csv"), package="RevoHDFS")
        hdfs.mkdir("/test/airline")
        checkTrue(hdfs.put(localData, "/test/airline/AirlineDemo1kNoMissing.csv"))
	checkTrue(hdfs.exists("/test/airline/AirlineDemo1kNoMissing.csv"))
        checkTrue(hdfs.exists("/test"))
        checkTrue(hdfs.exists("/test/airline"))
        checkTrue(!hdfs.exists("/no_such_file"))

        ##Breakdown
        hdfs.delete("/test")
}

