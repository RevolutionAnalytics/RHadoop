# Copyright 2011 Revolution Analytics
#    
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

 
stopifnot(require(rhdfs, quietly=TRUE))
stopifnot(require(RUnit, quietly=TRUE))

test.hdfs.write <- function()
{
        ## Load airline test data and put it into the Hadoop file system
        localData <- system.file(file.path("unitTestData", "AirlineDemo1kNoMissing.csv"), package="rhdfs")
	hdfs.mkdir("/test/airline")
        hdfs.put(localData, "/test/airline/AirlineDemo1kNoMissing.csv")
        ## read and write testing
	file1 <- hdfs.file("/test/airline/AirlineDemo1kNoMissing.csv","r")
	checkTrue(is.character(somedata <- rawToChar(hdfs.read(file1,100))))
        w.file <- hdfs.file("/test/airline/tmp/sample.csv","w")
	N <- 1000
	checkTrue(is.character(somedata <- rawToChar(hdfs.read(file1,N))))
	checkTrue(hdfs.write(somedata,w.file))
	checkTrue(hdfs.close(w.file))
	w.file <- hdfs.file("/test/airline/tmp/sample.csv","r")
	x <- c()
	while(TRUE){
  		if(is.null(a <- hdfs.read(w.file,N))) break
  		x <- c(x,a)
	}
	hdfs.close(w.file)
	checkTrue(all(somedata== unserialize(x)))

	## Breakdown
	hdfs.delete("/test")
}
