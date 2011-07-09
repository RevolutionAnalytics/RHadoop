stopifnot(require(RevoHDFS, quietly=TRUE))
stopifnot(require(RUnit, quietly=TRUE))

test.hdfs.mkdir <- function()
{
	checkTrue(hdfs.mkdir("/a/b"))
	checkTrue(hdfs.file.info("/a/b")$isDir)
	checkTrue(hdfs.mkdir("/test/airline"))
	checkTrue(hdfs.file.info("/test/airline")$isDir)
	hdfs.delete("/a")
	hdfs.delete("/test")
}
