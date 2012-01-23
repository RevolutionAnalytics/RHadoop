\name{mapreduce}
\alias{mapreduce}

\title{MapReduce using Hadoop Streaming}
\description{Defines and executes a map reduce job.
}
	
\usage{ mapreduce(
  input,
  output = NULL,
  map = to.map(identity),
  reduce = NULL,
  combine = NULL,
  reduce.on.data.frame = FALSE,
  input.specs = make.input.specs(),
  output.specs = make.output.specs(),
  tuning.parameters = list(),
  verbose = TRUE) }

\arguments{
\item{input}{Paths to the input folder(s) (on HDFS) or vector thereof
    or or the return value of another mapreduce or to.dfs call}
\item{output}{A path to the destination folder (on HDFS); if missing, use the return value as output}
\item{map}{An optional R function(k,v), returning the return value of keyval or list thereof, that specifies the map operation to execute as
    part of a map reduce job}
\item{reduce}{An optional R function(k,vv), returning the return value of keyval or list thereof, that specifies the reduce operation to
    execute as part of a map reduce job}
\item{combine}{turn on the combiner; if TRUE the reduce function is used, or specify your own}
\item{reduce.on.data.frame}{flatten the list of values to a data frame in the reduce call}
\item{input.specs}{Input specification, see make.input.specs}
\item{output.specs}{Output specification, see make.outout.specs}
\item{tuning.parameters}{Specify additional, backend-specific, performance-only-related options, as in \code{tuning.parameters = list(hadoop
      = list(D = "mapred.reduce.tasks=1"), local = list())}. It is invalid to use this argument to change the semantics of mapreduce (output
    should be independent of this argument). Each backend can only see the nested list named after the backend itself.  The interpretation
    is the following: for the hadoop backend, generate an additional hadoop streaming command line argument for each element of the list,
    "-name value". If the value is TRUE generate "-name" only, if it is FALSE skip. For the local backend, the list is ignored.}
  \item{verbose}{Run hadoop in verbose mode}}

  \value{The value of \code{output}, or, when missing, an object that can be used as input to \code{\link{from.dfs}} or \code{mapreduce}}

  \details{Defines and executes a map reduce job. Jobs can be chained together by simply providing the return value of one as input to the
    other. The map and reduce functions will run in an environment that is an approximation (explained below) of the environment of this
    call, even if the actual execution will happen in a different interpreter on a different machine. This is work in progress as we aim to
    provide exactly the same environement, but the current implementation seems to cover most use cases. Specifically, at \code{map} or
    \code{reduce} execution time, we load a copy of the environment inside the \code{mapreduce} call, as if \code{map} and \code{reduce}
    where called from there and its parent environment, that is the one in which \code{mapreduce} call is executed. We do not follow the
    environment chain further up or load the libraries that are loaded at the time of the call to \code{mapreduce}, but this can be improved
    upon in future versions. Changes to the above environemnts performed inside the map and reduce functions won't have any effects on the
    original environments, in a departure from established but rarely used R semantics. This is unlikely to change in the future because of
    the challenges inherent in adopting reference semantics in a parallel environment.}
  
\seealso{\code{\link{to.map}} and \code{\link{to.reduce}} can be used to convert other functions into suitable arguments for the map and
reduce arguments; see the inst and tests directories in the source for more examples}

\examples{ ## Example 1: Word Count ## classic wordcount ## input can be any text

wordcount = function (input, output, pattern = " ") {
 mapreduce(input = input ,
                   output = output,
                   input.specs = make.input.specs("text"),
                   map = function(k,v) {
                   lapply(
                     strsplit(
                       x = v,
                       split = pattern)[[1]],                    
                       function(w) keyval(w,1))},           
                  reduce = function(k,vv) {             
                  keyval(k, sum(unlist(vv)))})}

## Example 2:  Logistic Regression
## see spark implementation http://www.spark-project.org/examples.html
## see nice derivation here http://people.csail.mit.edu/jrennie/writing/lr.pdf

## create test set as follows
## to.dfs(lapply (1:100, function(i) {eps = rnorm(1, sd =10) ; keyval(i, list(x = c(i,i+eps), y = 2 * (eps > 0) - 1))}), "/tmp/logreg")
## run as:
## logistic.regression("/tmp/logreg", 10, 2)

logistic.regression = function(input, iterations, dims, alpha = -0.001){  
  plane = rep(0, dims)  
  g = function(z) 1/(1 + exp(-z))  
  for (i in 1:iterations) {    
    gradient = from.dfs(mapreduce(input,      
      map = function(k, v) keyval (1, v$y * v$x * g(-v$y * (plane \%*\% v$x))),    
      reduce = function(k, vv) keyval(k, apply(do.call(rbind,vv),2,sum))))    
    plane = plane + alpha * gradient[[1]]$val }  
  plane }                        

## Example 3:  K-Means Clustering

kmeans.iter =  
function(points, distfun, ncenters = length(centers), centers = NULL, summaryfun) {    
  centerfile = NULL
  mapreduce(input = points,             
  output= centerfile,             
  map = function(k,v) {               
    if (is.null(centers)) {                 
      keyval(sample(1:ncenters,1),v)}               
    else {                 
      distances = lapply(centers, function(c) distfun(c,v))                 
	keyval(centers[[which.min(distances)]], v)}},             
    reduce = function(k,vv) keyval(NULL, apply(do.call(rbind, vv), 2, mean)))    
    centers = from.dfs(centerfile)   }
  
kmeans =  
  function(points, ncenters, iterations = 10, distfun = function(a,b) norm(as.matrix(a-b), type = 'F'), summaryfun = mean) {    
    newCenters = kmeans.iter(points, distfun = distfun, ncenters = ncenters, summaryfun = summaryfun)    
    for(i in 1:iterations) {      
      newCenters = lapply(values(newCenters), unlist)      
      newCenters = kmeans.iter(points, distfun, centers=newCenters)}    
  newCenters  
}

## sample data, 12 clusters
## clustdata = lapply(1:100, function(i) keyval(i, c(rnorm(1, mean = i%%3, sd = 0.01), rnorm(1, mean = i%%4, sd = 0.01))))
## call with ## to.dfs(clustdata, "/tmp/clustdata")
## kmeans ("/tmp/clustdata", 12) 

}
