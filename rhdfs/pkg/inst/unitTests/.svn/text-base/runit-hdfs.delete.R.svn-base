stopifnot(require(RevoHDFS, quietly=TRUE))
stopifnot(require(RUnit, quietly=TRUE))

test.hdfs.delete <- function()
{
	## Load airline test data and put it into the Hadoop file system
        localData <- system.file(file.path("unitTestData", "AirlineDemo1kNoMissing.csv"), package="RevoHDFS")
        hdfs.mkdir("/test/airline")
        hdfs.put(localData, "/test/airline/AirlineDemo1kNoMissing.csv")

	## Actual test stuff
	checkTrue(hdfs.copy("/test/airline/AirlineDemo1kNoMissing.csv","/test/airline/temp1.csv"))
	checkTrue(hdfs.delete("/test/airline/temp1.csv"))

        ## Breakdown
	hdfs.delete("/test")
}
