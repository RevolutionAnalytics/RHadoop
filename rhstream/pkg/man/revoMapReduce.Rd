\name{revoMapReduce}
\alias{revoMapReduce}

\title{MapReduce using Hadoop Streaming}
\description{Defines and executes a map reduce job.
}
	
\usage{ revoMapReduce(
  input,
  output = NULL,
  map = mkMap(identity),
  reduce = NULL,
  reduceondataframe = FALSE,
  combine = NULL,
  profilenodes = FALSE,
  inputformat = NULL,
  textinputformat = defaulttextinputformat,
  textoutputformat = defaulttextoutputformat,
  verbose = FALSE) }

\arguments{
\item{input}{Paths to the input folder(s) (on HDFS) or vector thereof
    or or the return value of another revoMapReduce or rhwrite call}
  \item{output}{A path to the destination folder (on HDFS); if
      missing, use the return value as output}
  \item{map}{An optional R function(k,v), returning the return value
      of keyval or list thereof, that specifies the map operation to
      execute as part of a map reduce job}
  \item{reduce}{An optional R function(k,vv), returning the return
      value of keyval or list thereof, that specifies the reduce
      operation to execute as part of a map reduce job}
  \item{combine}{turn on the combiner; if TRUE the reduce function is
      used, or specify your own}
  \item{inputformat}{Can be the fully qualified Java class, in which
      case the JAR file must be passed via \code{jarfiles}}
  \item{textinputformat}{a function generating a key-value pair from a
      line of text according to some format convention}
  \item{textoutputformat}{a function generating a line of text from a
      keyvalue pair according to some format convention}
  \item{reduceondataframe}{flatten the list of values to a data frame
      in the reduce call}
  \item{profilenodes}{turn on the profiler on the mapper and reducer;
      output in /tmp/Rprof/<job_id>/<attempt_id>}
  \item{verbose}{Run hadoop in verbose mode}}

\value{The value of \code{output}, or, when missing,  an object that
  can be used as input to \code{\link{rhread}} or \code{revoMapReduce}}

\details{Defines and executes a map reduce job. Jobs can be chained
  together by simply providing the return value of one as input to
  the other. The map and reduce functions will run in an environment
  that is an approximation (explained below) of the environment of this call, even if
  the actual execution will happen in a different interpreter on a
  different machine. This is work in progress as we aim to provide
  exactly the same environement, but the current implementation seems
  to cover most use cases. Specifically, at \code{map} or
  \code{reduce} execution time,  we load  a copy of the environment
  inside the \code{revoMapReduce} call, as if \code{map} and
  \code{reduce} where called from there and its parent environment,
  that is the one in which \code{revoMapReduce} call is executed. We do not
  follow the environment chain further up or load the libraries that
  are loaded at the time of the call to \code{revoMapReduce}, but this
  can be improved upon in future versions. Changes
  to the above environemnts performed inside the map and reduce
  functions won't have any effects on the original environments, in a
  departure from established but rarely used R semantics. This is
  unlikely to change in the future because of the challenges inherent
  in adopting reference semantics in a parallel environment.}
  
\seealso{\code{\link{mkMap}} and \code{\link{mkReduce}} can be used to
convert other functions into suitable arguments for the map and reduce
arguments; \code{\link{rawtextinputformat}} shows an alternative text
input format specification. See the inst and tests directories in the
source for more examples}

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
      map = function(k, v) keyval (1, v$y * v$x * g(-v$y * (plane \%*\% v$x))),    
      reduce = function(k, vv) keyval(k, apply(do.call(rbind,vv),2,sum))))    
    plane = plane + alpha * gradient[[1]]$val }  
  plane }                        

## Example 3:  K-Means Clustering

rhkmeansiter =  
function(points, distfun, ncenters = length(centers), centers = NULL, summaryfun) {    
  centerfile = NULL
  revoMapReduce(input = points,             
  output= centerfile,             
  map = function(k,v) {               
    if (is.null(centers)) {                 
      keyval(sample(1:ncenters,1),v)}               
    else {                 
      distances = lapply(centers, function(c) distfun(c,v))                 
	keyval(centers[[which.min(distances)]], v)}},             
    reduce = function(k,vv) keyval(NULL, apply(do.call(rbind, vv), 2, mean)))    
    centers = rhread(centerfile)   }
  
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
