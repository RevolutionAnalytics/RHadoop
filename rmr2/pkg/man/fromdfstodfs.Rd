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
  \item{input}{A file in HDFS or other storage layer, as specified by active backend, to read from or the return value of \code{mapreduce}}
  \item{output}{A file in HDFS or other storage layer, as specified by active backend, to read from}
  \item{format}{For \code{from.dfs} either a string naming a format, the same as those allowed by \code{make.input.format}, or the value returned by \code{\link{make.input.format}}. The same is true for \code{to.dfs}, but refer to \code{\link{make.output.format}} instead.}}

\details{ These functions allow to move data from RAM to the file system and back. Keep in mind that the capacity of these two storage media is
different by two or more orders of magnitude, so the conversion will make sense only in specific situations. These
functions do not perform any size control, so the responsibility is on the user. For the local backend, file system means the local file system.
For the Hadooop backend it means HDFS}

\value{\code{from.dfs} returns the object whose representation is contained in \code{file}. \code{to.dfs} returns the file it wrote a
representation of the object provided as argument to or, when \code{output} is missing, an object that can be passed as input to a \code{mapreduce}
or \code{from.dfs} call.  }

\examples{
from.dfs(to.dfs(1:10))
from.dfs(to.dfs(keyval(1, 1:10)))
}
