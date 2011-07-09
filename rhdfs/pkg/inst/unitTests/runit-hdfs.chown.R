stopifnot(require(RevoHDFS, quietly=TRUE))
stopifnot(require(RUnit, quietly=TRUE))

test.hdfs.chown <- function()
{
        ## Load airline test data and put it into the Hadoop file system
        localData <- system.file(file.path("unitTestData", "AirlineDemo1kNoMissing.csv"), package="RevoHDFS")
        hdfs.mkdir("/test/airline")
        checkTrue(hdfs.put(localData, "/test/airline/AirlineDemo1kNoMissing.csv"))
        currOwner <- as.character(hdfs.ls("/test/airline/AirlineDemo1kNoMissing.csv")$owner)
	hdfs.chown("/test/airline/AirlineDemo1kNoMissing.csv", owner="nobody")
	checkTrue(as.character(hdfs.ls("/test/airline/AirlineDemo1kNoMissing.csv")$owner)=="nobody")
	hdfs.chown("/test/airline/AirlineDemo1kNoMissing.csv", owner=currOwner)
        currOwner <- as.character(hdfs.ls("/test")$owner)
        hdfs.chown("/test/airline", owner="nobody")
        checkTrue(as.character(hdfs.ls("/test")$owner)=="nobody")
        hdfs.chown("/test/airline", owner=currOwner)

        ##Breakdown
        hdfs.delete("/test")
}

