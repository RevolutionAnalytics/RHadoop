stopifnot(require(RevoHDFS, quietly=TRUE))
stopifnot(require(RUnit, quietly=TRUE))

test.hdfs.defaults <- function()
{
	x <- hdfs.defaults()
	checkTrue( !is.null(x$local)) 
	checkTrue(!is.null(x$blocksize))
	checkTrue(!is.null(x$fs))
	checkTrue(!is.null(x$fs))
	checkTrue(!is.null(x$fu))
	checkTrue(!is.null(x$classpath))
        if (length(grep("Distributed", .jclass(x$fs)))){
		checkTrue( .jclass(x$fs) == "org.apache.hadoop.hdfs.DistributedFileSystem" )
	} else {
		checkTrue( .jclass(x$fs) ==  "org.apache.hadoop.fs.LocalFileSystem" )
	}
	checkTrue( .jclass(x$local) == "org.apache.hadoop.fs.LocalFileSystem" )
	checkTrue( attr(x$fu,"name") == "com.revolutionanalytics.hadoop.hdfs.FileUtils" )
	checkTrue( .jclass(x$conf) ==  "org.apache.hadoop.conf.Configuration")
	rm(x)
}
