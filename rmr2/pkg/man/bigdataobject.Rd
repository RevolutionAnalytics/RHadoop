\name{big.data.object}
\alias{big.data.object}

\title{
The big data object.}

\description{
A stub representing data on disk that can be manipulated by other functions in rmr. "Stub" means that the data is not actually "there" or more concretely it is not held in memory in the current process. This is a technique used in different programming languages when remote resources need to be made available. In this case the rationale is that we need to be able to process large data sets whose size is not compatible with them being held in memory at once. Nonetheless it is convenient to be able to refer to the complete data set in the language, albeit the set of operations we can perform on it is limited. Big data  objects are returned by \code{\link{to.dfs}}, \code{\link{mapreduce}}, \code{\link{scatter}}, \code{\link{gather}}, \code{\link{equijoin}} and \code{\link{rmr.sample}}, and accepted as input by all of the above with the exception of \code{\link{to.dfs}} and the inclusion of \code{\link{from.dfs}}. Big data objects are NOT persistent, meaning that they are not meant to be saved beyond the limits of a session. They use temporary space and the space is reclaimed as soon as possible when the data can not be referred to any more, or at the end of a session. For data that needs to be accessible outside the current R session, you need to use paths to the file or directory where the data is or should be written to. Valid paths can be used interchangeably wherever big data objects are accepted}


\examples{
some.big.data = to.dfs(1:10)
to.dfs(1:10, "/tmp/some/big/data")}
