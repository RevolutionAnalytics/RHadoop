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
  input.format = "native",
  output.format = "native",
  backend.parameters = list(),
  verbose = TRUE) }

\arguments{
\item{input}{Paths to the input folder(s) (on HDFS) or vector thereof
    or or the return value of another \code{mapreduce} or a \code{\link{to.dfs}} call}
\item{output}{A path to the destination folder (on HDFS); if missing, a \code{\link{big.data.object}} is returned, see "Value" below}
\item{map}{An optional R function of two arguments, returning either NULL or the return value of \code{\link{keyval}}, that specifies the map operation to execute as part of a mapreduce job. The two arguments represent multiple key-value pairs according to the definition of the mapreduce model. They can be any of the following: list, vector, matrix, data frame or NULL (the last one only allowed for keys). Keys are matched to the corresponding values by position, according to the second dimension if it is defined (that is rows in matrices and data frames, position otherwise), analogous to the behavior of \code{cbind}, see \code{\link{keyval}} for details.}
\item{reduce}{An optional R function of two arguments, a key and a data structure representing all the values associated with that key (the same type as returned by the map call, merged with \code{rbind} for matrices and data frames and \code{c} otherwise), returning either NULL or the return value of \code{\link{keyval}}, that specifies the reduce operation to  execute as part of a mapreduce job. The default is no reduce phase, that is the output of the map phase is the output of the mapreduce job}
\item{combine}{A function with the same signature and possible return values as the reduce function, or TRUE, which means use the reduce function as combiner. NULL or FALSE means no combiner is used.}
\item{input.format}{Input format specification, see  \code{\link{make.input.format}}}
\item{output.format}{Output format specification, see  \code{\link{make.output.format}}}
\item{backend.parameters}{This option is for advanced users only and may be removed in the future. Specify additional, backend-specific
    options, as in \code{backend.parameters = list(hadoop  = list(D = "mapred.reduce.tasks=1"), local = list())}. It is recommended not to use this argument to change the semantics of mapreduce (output should be independent of this argument). Each backend can only see the nested list named after the backend itself.  The interpretation is the following: for the hadoop backend, generate an additional hadoop streaming command line argument for each element of the list, "-name value". If the value is TRUE generate "-name" only, if it is FALSE skip. One possible use is to specify the number of mappers and reducers on a per-job basis. It is not guaranteed that the generated streaming command will be a legal command. In particular, remember to put any generic options before any specific ones, as per hadoop streaming manual. For the local backend, the list is currently ignored.}
  \item{verbose}{Run hadoop in verbose mode. No effect on the local backend}}

  \value{The value of \code{output}, or, when missing, a \code{\link{big.data.object}}}

  \details{Defines and executes a mapreduce job. Jobs can be chained together by simply providing the return value of one as input to the
    other. The map and reduce functions will run in an environment that is a close approximation of the environment of this
    call, even if the actual execution happens in a different interpreter on a different machine.  Changes to the outer
    environemnts performed inside the map and reduce functions with the \code{<<-} operator will only affect a per-process copy of the
    environment, not the original one, in a departure from established but seldom used R semantics. This is unlikely to change in the future
    because of the challenges inherent in adopting reference semantics in a parallel environment. See also the Tutorial
    \url{https://github.com/RevolutionAnalytics/RHadoop/wiki/Tutorial}}
  
\seealso{\code{\link{to.map}} and \code{\link{to.reduce}} can be used to convert other functions into suitable arguments for the map and
reduce arguments; see the tests directory in the package for more examples}

