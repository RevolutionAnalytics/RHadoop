\name{rmr.profilenodes}
\alias{rmr.profilenodes}
\title{
Turn on profiling on the nodes.
}
\description{
Collect profiling information when running additional R interpreters (besides the current one) on the cluster. Show or change setting.
}

\usage{
rmr.profilenodes(on = NULL)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{on}{
A boolean, tells whether profiling should be on.
}
}

\details{Profiling data is collected in the following file: \code{file.path("/tmp/Rprof", Sys.getenv('mapred_job_id'),
Sys.getenv('mapreduce_tip_id'))}. Can be turned on with an argument to mapreduce or globally with this call.}

\value{
A boolean, whether profiling is on.
}

\examples{
rmr.profilenodes(TRUE)
rmr.profilenodes(FALSE)
}