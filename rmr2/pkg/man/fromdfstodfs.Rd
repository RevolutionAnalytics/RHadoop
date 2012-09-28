\name{from.dfs}
\alias{from.dfs}
\alias{to.dfs}

\title{Read or write R objects from or to the file system}
\description{Functions that read or write R objects from or to the file system}

\usage{
to.dfs(kv, output = dfs.tempfile(), format = "native")
from.dfs(input, format = "native")
}

\arguments{
  \item{kv}{A key-value pair; also, a vector, list, matrix or a data frame (in this case the keys will be set to NULL)}
  \item{input}{A valid path or a \code{\link{big.data.object}}}
  \item{output}{A valid path}
  \item{format}{For \code{from.dfs} either a string naming a format, the same as those allowed by \code{make.input.format}, or the value returned by \code{\link{make.input.format}}. The same is true for \code{to.dfs}, but refer to \code{\link{make.output.format}} instead.}}

\details{ These functions allow to move data from RAM to the file system and back. Keep in mind that the capacity of these two storage media is
different by two or more orders of magnitude, so the conversion will make sense only in specific situations. These
functions do not perform any size control, so the responsibility is on the user. For the local backend, file system means the local file system.
For the Hadooop backend it means HDFS}

\value{\code{from.dfs} returns the object whose representation is contained in \code{input}. \code{to.dfs} returns the value of \code{output} or, when this is missing, a \code{\link{big.data.object}}  }

\examples{
from.dfs(to.dfs(1:10))
from.dfs(to.dfs(keyval(1, 1:10)))
}
