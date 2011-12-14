\name{rmr.options.get}
\alias{rmr.options.get}
\alias{rmr.options.set}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Functions to set and get package options}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
}
\usage{
rmr.options.get(...)
rmr.options.set(backend = c(NULL, "hadoop", "local"), profile.nodes = NULL, depend.check = NULL, managed.dir = NULL)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{backend}{
One of "hadoop" or "local", the latter being implemented entirely in the current R interpreter, sequentially.
}
  \item{profile.nodes}{
Collect profiling information when running additional R interpreters (besides the current one) on the cluster. }
  \item{depend.check}{
Activate makefile-like dependency checking}
  \item{managed.dir}{
Where to put intermediate result when makefile-like features are activated}
}
}
\details{
 Mapreduce has come to mean massive, fault tolerant distributed computing because of its use by Google and Hadoop, but it is also
an abstract model of computation amenable to different implementations. Here we provide access to Hadoop through the hadoop backend and
provide an all-R, single interpreter implementation that's good for experimentation and debugging, in particular to debug mapper and
reducers. Can be specified as an argument to mapreduce or globally with this call. Profiling data is collected in the following file: \code{file.path("/tmp/Rprof", Sys.getenv('mapred_job_id'),
Sys.getenv('mapreduce_tip_id'))}. Describe dependency checking here }
\value{
A named list with the options and their values, or just a value if only one requested.
}


\examples{

}
