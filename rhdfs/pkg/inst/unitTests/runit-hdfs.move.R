stopifnot(require(RevoHDFS, quietly=TRUE))
stopifnot(require(RUnit, quietly=TRUE))

test.hdfs.move <- function()
{
        ## Load airline test data and put it into the Hadoop file system
        localData <- system.file(file.path("unitTestData", "AirlineDemo1kNoMissing.csv"), package="RevoHDFS")
        hdfs.mkdir("/test/airline")
        hdfs.put(localData, "/test/airline/AirlineDemo1kNoMissing.csv")
	## test checks
	checkTrue(hdfs.move("/test/airline/AirlineDemo1kNoMissing.csv","/test/airline/temp.csv"))
	checkTrue(hdfs.move("/test/airline/temp.csv","/test/airline/AirlineDemo1kNoMissing.csv"))

        ##Breakdown
        hdfs.delete("/test")
}

