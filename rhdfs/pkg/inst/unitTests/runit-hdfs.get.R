stopifnot(require(RevoHDFS, quietly=TRUE))
stopifnot(require(RUnit, quietly=TRUE))

test.hdfs.get <- function()
{
	x <- hdfs.defaults()
	y <- tempdir()
	## Load airline test data and put into Hadoop file system
        localData <- system.file(file.path("unitTestData", "AirlineDemo1kNoMissing.csv"), package="RevoHDFS")
        hdfs.mkdir("/test/airline")
        hdfs.put(localData, "/test/airline/AirlineDemo1kNoMissing.csv")

	checkTrue(hdfs.get("/test/airline/AirlineDemo1kNoMissing.csv",y))
	checkTrue("AirlineDemo1kNoMissing.csv" %in% list.files(y))
	unlink(y)

	## Breakdown
	hdfs.delete("/test")
}
