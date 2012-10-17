\name{rmr.options}
\alias{rmr.options}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Function to set and get package options}
\description{
set and get package options}
\usage{
rmr.options(backend = c("hadoop", "local"), profile.nodes = FALSE, keyval.length = 1000, install.args = NULL, update.args = NULL)
%-, depend.check = NULL, managed.dir = NULL)
}
\arguments{
  \item{...}{Names of options to get values of, as character}
  \item{backend}{One of "hadoop" or "local", the latter being implemented entirely in the current R interpreter, sequentially, for learning and debugging.}
  \item{profile.nodes}{Collect profiling information when running additional R interpreters (besides the current one) on the cluster. No effect on the local backend, use Rprof instead}
  \item{keyval.length}{How many records to read into a single, key-value pair or how many to write to when the key is \code{NULL}. Controls both \code{\link{from.dfs}} and \code{\link{to.dfs}} and \code{\link{mapreduce}} behavior.}
 %\item{depend.check}{Activate makefile-like dependency checking (under construction)}
 % \item{managed.dir}{Where to put intermediate result when makefile-like features are activated}
  \item{install.args}{Additional arguments for \code{install.package} when attempting to install missing packages on the nodes at the beginning of a mapreduce job. The default, \code{NULL}, disables any installationn. rmr will then just issue a warning for missing packages and try to continue, hoping that they are not necessary.}
  \item{update.args}{Additional arguments for \code{update.packages} when attempting to install missing packages on the nodes at the beginning of a mapreduce job. The default, \code{NULL}, disables any update. `ask = FALSE` is passed independent of the setting of this option}
}
\details{
 Mapreduce has come to mean massive, fault tolerant distributed computing because of its use by Google and Hadoop, but it is also
an abstract model of computation amenable to different implementations. Here we provide access to mapreduce through the hadoop backend and
provide an all-R, single interpreter implementation (local) that's good for experimentation and debugging, in particular to debug mappers and
reducers. Profiling data is collected in the following files: \code{file.path("/tmp/Rprof", Sys.getenv('mapred_job_id'), Sys.getenv('mapreduce_tip_id'))} on each node. 
%Describe dependency checking here 
}
\value{A named list with the options and their values, or just a value if only one requested. NULL when only setting options.}


\examples{
old.backend = rmr.options("backend")
rmr.options(backend = "hadoop")
rmr.options(backend = old.backend)
}
