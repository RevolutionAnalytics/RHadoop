stopifnot(require(RevoHDFS, quietly=TRUE))
stopifnot(require(RUnit, quietly=TRUE))

test.hdfs.chmod <- function()
{
        ## Load airline test data and put it into the Hadoop file system
        localData <- system.file(file.path("unitTestData", "AirlineDemo1kNoMissing.csv"), package="RevoHDFS")
        hdfs.mkdir("/test/airline")
        checkTrue(hdfs.put(localData, "/test/airline/AirlineDemo1kNoMissing.csv"))
	hdfs.chmod("/test/airline/AirlineDemo1kNoMissing.csv", permissions="664")
	checkTrue(as.character(hdfs.ls("/test/airline")$permission) == "-rw-rw-r--")
        ##Breakdown
        hdfs.delete("/test")
}

