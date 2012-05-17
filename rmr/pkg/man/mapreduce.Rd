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
  input.format = "native",
  output.format = "native",
  vectorized = list(map = FALSE, reduce = FALSE),
  structured = list(map = FALSE, reduce = FALSE),
  backend.parameters = list(),
  verbose = TRUE) }

\arguments{
\item{input}{Paths to the input folder(s) (on HDFS) or vector thereof
    or or the return value of another \code{mapreduce} or a \code{\link{to.dfs}} call}
\item{output}{A path to the destination folder (on HDFS); if missing, use the return value of \code{mapreduce} itself as output}
\item{map}{An optional R function of two arguments, a key and a value, returning either NULL or the return value of \code{\link{keyval}} or a list thereof, that specifies the map operation to execute as part of a mapreduce job}
\item{reduce}{An optional R function of two arguments, a key and a list of all the values associated with that key, returning either NULL or the return value of \code{\link{keyval}} or a list thereof, that specifies the reduce operation to
    execute as part of a map reduce job}
\item{combine}{A function with the same signature as the reduce function, or TRUE, which means use the reduce function as combiner}
\item{reduce.on.data.frame}{DEPRECATED. Use \code{structured} instead. Flatten the list of values to a data frame in the reduce call}
\item{input.format}{Input specification, see  \code{\link{make.input.format}}}
\item{output.format}{Output specification, see  \code{\link{make.output.format}}}
\item{vectorized}{Whether to process multiple records in a single function call. Its value is a list with two named entries, \code{map} and \code{reduce}. The reduce entry is there for future use only. The map entry can be a boolean or an integer specifying the exact number of records to be processed (defaults to 1000 when set to TRUE). In this case the map function arguments will be equal length lists of keys and values, unless \code{structured$map} is specified (see that option). Available shorthands are to set \code{vectorized} to an integer or a logical value, in which case they will apply to both the map and reduce phases (the latter in future versions only)}
\item{structured}{Whether the input to map or reduce is structured data. This applies to the map function only when \code{structured$map} is \code{TRUE} and \code{vectorized$map} is \code{TRUE} or greater than 1 and will try to coerce the list of keys and the list of values to data frames. It is on by default for certain formats that can only represent structured data such as "csv"" (this statement is more or less accurate depending on what you mean by structured data, but here we take it to mean that it can represented by a data frame with atomic columns). \code{reduce$map} applies only to the list of values passed to the reduce function, also converted to a data frame when possible. This replaces the now deprecated \code{reduce.on.data.frame}. An available shortand is to set \code{structured} to a logical value, in which case it will affect both the map and reduce phases}
\item{backend.parameters}{Specify additional, backend-specific options, as in \code{backend.parameters = list(hadoop
      = list(D = "mapred.reduce.tasks=1"), local = list())}. It is recommended not to use this argument to change the semantics of mapreduce (output
    should be independent of this argument). Each backend can only see the nested list named after the backend itself.  The interpretation
    is the following: for the hadoop backend, generate an additional hadoop streaming command line argument for each element of the list,
    "-name value". If the value is TRUE generate "-name" only, if it is FALSE skip. One possible use is to specify the number of mappers and reducers 
    on a per-job basis. It is not guaranteed that the generated streaming command will be a legal command. In particular, remember to put any generic 
    options before any specific ones, as per hadoop streaming manual.For the local backend, the list is ignored.}
  \item{verbose}{Run hadoop in verbose mode}}

  \value{The value of \code{output}, or, when missing, an object that can be used as input to \code{\link{from.dfs}} or \code{mapreduce}, a stub representing the results of the job}

  \details{Defines and executes a mapreduce job. Jobs can be chained together by simply providing the return value of one as input to the
    other. The map and reduce functions will run in an environment that is an approximation of the environment of this
    call, even if the actual execution happens in a different interpreter on a different machine.  Changes to the outer
    environemnts performed inside the map and reduce functions with the \code{<<-} operator will only affect a per-process copy of the
    environment, not the original one, in a departure from established but rarely used R semantics. This is unlikely to change in the future
    because of the challenges inherent in adopting reference semantics in a parallel environment. See also the Tutorial
    \url{https://github.com/RevolutionAnalytics/RHadoop/wiki/Tutorial}}
  
\seealso{\code{\link{to.map}} and \code{\link{to.reduce}} can be used to convert other functions into suitable arguments for the map and
reduce arguments; see the inst and tests directories in the source package for more examples}

\examples{ ## Example 1: Word Count ## classic wordcount ## input can be any text

wordcount = function (input, output, pattern = " ") {
 mapreduce(input = input ,
                   output = output,
                   input.format = make.input.format("text"),
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
