\name{revoMapReduce}
\alias{rhstream}

\title{MapReduce using Hadoop Streaming}
\description{Applies a Map R function to blocks of text data and then Reduces them using R functions
}
	
\usage{
revoMapReduce <- function(input,output, map, reduce = NULL, verbose=FALSE, inputformat=NULL, textinputformat=defaulttextinputformat, textoutputformat=defaulttextoutputformat)
}

\arguments{
\item{input}{A path to the input folder (on HDFS)}
\item{output}{A path to the destination folder  (on HDFS}
\item{map}{An R function that specifies the map operation to execute in the Hadoop streaming job}
\item{reduce}{An optional R function that specifies the reduce operation to execute in the Hadoop streaming job}
\item{inputformat}{Can be the fully qualified Java class, in which case the JAR file must be passed via \code{jarfiles}}
\item{textinputformat}{}
\item{textoutputformat}{}

\details{ }

\examples{

## Example 1:  Word Count
## classic wordcount 
## input can be any text 

mrwordcount = function (input, output, pattern = " ") {
  revoMapReduce(input = input ,
                output = output,
                textinputformat = rawtextinputformat,
                map = function(k,v) {
                  lapply(
                    strsplit(
                      x = v,
                      split = pattern)[[1]],                    
                      function(w) keyval(w,1))},           
                reduce = function(k,vv) {             
                  keyval(k, sum(unlist(vv)))}
               )
}

## Example 2:  Logistic Regression
## see spark implementation http://www.spark-project.org/examples.html
## see nice derivation here http://people.csail.mit.edu/jrennie/writing/lr.pdf

## create test set as follows
## rhwrite(lapply (1:100, function(i) {eps = rnorm(1, sd =10) ; keyval(i, list(x = c(i,i+eps), y = 2 * (eps > 0) - 1))}), "/tmp/logreg")
## run as:
## rhLogisticRegression("/tmp/logreg", 10, 2)

rhLogisticRegression = function(input, iterations, dims, alpha = -0.001){  
  plane = rep(0, dims)  
  g = function(z) 1/(1 + exp(-z))  
  for (i in 1:iterations) {    
    gradient = rhread(revoMapReduce(input,      
      map = function(k, v) keyval (1, g(-v$y * (plane %*% v$x)) * v$y * v$x),      
      reduce = function(k, vv) keyval(k, apply(do.call(rbind,vv),2,sum))))    
    plane = plane + alpha * gradient[[1]]$val }  
  plane }                        


}

## Example 3:  K-Means Clustering

rhkmeansiter =  
  function(points, distfun, ncenters = length(centers), centers = NULL, summaryfun) {    
	centerfile = hdfs.tempfile()    
    revoMapReduce(input = points,             
	              output= centerfile,             
				  map = function(k,v) {               
					  if (is.null(centers)) {                 
						  keyval(sample(1:ncenters,1),v)}               
					  else {                 
						  distances = lapply(centers, function(c) distfun(c,v))                 
					      keyval(centers[[which.min(distances)]], v)}},             
				  reduce = function(k,vv) keyval(NULL, apply(do.call(rbind, vv), 2, mean)))    
    centers = rhread(centerfile)  
  }

rhkmeans =  
  function(points, ncenters, iterations = 10, distfun = function(a,b) norm(as.matrix(a-b), type = 'F'), summaryfun = mean) {    
	newCenters = rhkmeansiter(points, distfun = distfun, ncenters = ncenters, summaryfun = summaryfun)    
    for(i in 1:iterations) {      
		newCenters = lapply(RevoHStream:::getValues(newCenters), unlist)      
	    newCenters = rhkmeansiter(points, distfun, centers=newCenters)}    
	newCenters  
  }

## sample data, 12 clusters
## clustdata = lapply(1:100, function(i) keyval(i, c(rnorm(1, mean = i%%3, sd = 0.01), rnorm(1, mean = i%%4, sd = 0.01))))
## call with
## rhwrite(clustdata, "/tmp/clustdata")
## rhkmeans ("/tmp/clustdata", 12) 

}