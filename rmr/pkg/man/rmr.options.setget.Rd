\name{rmr.options.get}
\alias{rmr.options.get}
\alias{rmr.options.set}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Functions to set and get package options}
\description{
set and get package options}
\usage{
rmr.options.get(...)
rmr.options.set(backend = c("hadoop", "local"), profile.nodes = NULL, vectorized.keyval.length = NULL)
%-, depend.check = NULL, managed.dir = NULL)
}
\arguments{
  \item{...}{Character names of option to get the value of}
  \item{backend}{One of "hadoop" or "local", the latter being implemented entirely in the current R interpreter, sequentially. Very useful for learning and debugging.}
  \item{profile.nodes}{Collect profiling information when running additional R interpreters (besides the current one) on the cluster. }
  \item{vectorized.keyval.length}{How many records to read into a single, vectorized, key-value pair.}
 %\item{depend.check}{Activate makefile-like dependency checking (under construction)}
 % \item{managed.dir}{Where to put intermediate result when makefile-like features are activated}
}
\details{
 Mapreduce has come to mean massive, fault tolerant distributed computing because of its use by Google and Hadoop, but it is also
an abstract model of computation amenable to different implementations. Here we provide access to mapreduce through the hadoop backend and
provide an all-R, single interpreter implementation (local) that's good for experimentation and debugging, in particular to debug mappers and
reducers. Profiling data is collected in the following file: \code{file.path("/tmp/Rprof", Sys.getenv('mapred_job_id'), Sys.getenv('mapreduce_tip_id'))}. 
%Describe dependency checking here 
}
\value{A named list with the options and their values, or just a value if only one requested.}


\examples{
old.backend = rmr.options.get("backend")
rmr.options.set(backend = "hadoop")
rmr.options.set(backend = old.backend)
}
