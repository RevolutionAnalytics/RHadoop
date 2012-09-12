\name{dfs.empty}
\alias{dfs.empty}
\title{
Check if a dfs file is empty}

\description{
 Check if a dfs file is empty}
 
\usage{
dfs.empty(f)}

\arguments{
  \item{f}{A valid path or \code{\link{big.data.object}}}}

\value{A logical}

\examples{
dfs.empty(mapreduce(to.dfs(1:10)))
}