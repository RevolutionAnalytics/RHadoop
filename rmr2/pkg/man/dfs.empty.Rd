\name{dfs.empty}
\alias{dfs.empty}
\alias{dfs.size}
\title{
Get a directory or file size or check if it is empty}

\description{
 Check if a dfs file is empty}
 
\usage{
dfs.empty(f)
dfs.size(f)}

\arguments{
  \item{f}{A valid path or \code{\link{big.data.object}}}}

\value{For \code{dfs.size} an number of bytes; for \code{dfs.empty} a logical}

\details{
The size of a directory, for the sake of this commands, is the size of the files contained therein with the exception of hidden files starting with "." and "_". This is not well documented in Hadoop but there is a private call that implements this pattern. }

\examples{
dfs.empty(mapreduce(to.dfs(1:10)))
dfs.size(mapreduce(to.dfs(1:10)))
}