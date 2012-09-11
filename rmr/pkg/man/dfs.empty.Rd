\name{dfs.empty}
\alias{dfs.empty}
\title{
Check if a dfs file is empty
}
\description{
 Accepts strings or mapreduce return values/
}
\usage{
dfs.empty(f)
}
\arguments{
  \item{f}{
An file path or object returned by mapreduce
}
}


\value{
A boolean
}
\examples{
dfs.empty(mapreduce(to.dfs(1:10)))
}