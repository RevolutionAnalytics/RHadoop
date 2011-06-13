stopifnot(require(RevoHDFS, quietly=TRUE))
stopifnot(require(RUnit, quietly=TRUE))

test.hdfs.ls <- function()
{
        ## Load airline test data and put it into the Hadoop file system
        localData <- system.file(file.path("unitTestData", "AirlineDemo1kNoMissing.csv"), package="RevoHDFS")
        hdfs.mkdir("/test/airline")
        hdfs.put(localData, "/test/airline/AirlineDemo1kNoMissing.csv")
	hdfs.mkdir("/a/b")
	checkException(hdfs.ls("/___not-present___"))
	checkTrue(is.null(hdfs.ls("/a/b/")))
	checkTrue(hdfs.ls("/test",rec=FALSE)$file== "/test/airline")


        ##Breakdown
	hdfs.delete("/a")
        hdfs.delete("/test")
}


