\name{from.dfs}
\alias{from.dfs}
\alias{to.dfs}

\title{Read and write R lists to HDFS}
\description{Utility function that reads and writes lists to HDFS}

\usage{
to.dfs(object, output = dfs.tempfile(), format = "native")
from.dfs(input, format = "native", to.data.frame = FALSE)
}

\arguments{
  \item{object}{A list of key-value pairs as returned by keyvalue to write to HDFS; also, experimentally, a data frame}
  \item{input}{A file in HDFS to read from or the return value of \code{mapreduce}}
  \item{output}{A file in HDFS to read from}
  \item{format}{For \code{from.dfs} either a string naming a format, the same as those allowed by \code{make.input.format}, or the value returned by \code{\link{make.input.format}}. The same is true for \code{to.dfs}, but refer to \code{\link{make.output.format}} instead.}
  \item{to.data.frame}{Do what it takes to return a data frame.}
}



\details{ These functions allow to move data from RAM to HDFS and back. Keep in mind that the capacity of these two storage media is
different by two or three orders of magnitude, roughly speaking, so the conversion will make sense only in specific situations. These
functions do not perform any size control, so the responsibility is on the user.}

\value{\code{from.dfs} returns the object whose representation is contained in \code{file}. \code{to.dfs} returns the file it wrote a
representation of the object provided as argument to or, when \code{output} is missing, an object that can be passed as input to a \code{mapreduce}
or \code{from.dfs} call.  }

\examples{##See \code{\link{mapreduce}} for examples}
