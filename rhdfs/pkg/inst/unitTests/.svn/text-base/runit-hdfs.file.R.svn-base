stopifnot(require(RevoHDFS, quietly=TRUE))
stopifnot(require(RUnit, quietly=TRUE))

test.hdfs.file <- function()
{
        ## Load airline test data and put it into the Hadoop file system
        localData <- system.file(file.path("unitTestData", "AirlineDemo1kNoMissing.csv"), package="RevoHDFS")
        hdfs.mkdir("/test/airline")
        hdfs.put(localData, "/test/airline/AirlineDemo1kNoMissing.csv")

	## Unit Test Check
	checkTrue(class(hdfs.file("/test/airline/AirlineDemo1kNoMissing.csv","r"))=="hdfsFH")

        ##Breakdown
        hdfs.delete("/test")
}

