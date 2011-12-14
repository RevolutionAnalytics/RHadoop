\name{from.dfs}
\alias{from.dfs}
\alias{to.dfs}

\title{Read and write R lists to HDFS}
\description{Utility function that reads and writes lists to HDFS}

\usage{
to.dfs(object, file = dfs.tempfile(), text.output.format = default.text.output.format)
from.dfs(file, text.input.format = default.text.input.format, todataframe = F)
}

\arguments{
  \item{object}{An list of key-value pairs as returned by keyvalue to write to HDFS; experimentally, a data frame}
  \item{file}{A file in HDFS to read from or write to; for from.dfs, the return value of \code{mapreduce}}
  \item{text.output.format}{The output format of the file being written (from.dfs only)}
  \item{text.input.format}{The input format of the file being read (from.dfs only)}
  \item{todataframe}{Do what it takes to return a data frame (from.dfs only). }
}



\details{ These functions allow to move data from RAM to HDFS and back. Keep in mind that the capacity of these two storage media is
different by two or three orders of magnitude, roughly speaking, so the conversion will make sense only in specific situations. These
functions do not perform any size control, so the responsibility is on the user.}

\value{\code{from.dfs} returns the object whose representation is contained in \code{file}. \code{from.dfs} returns the file it wrote a
representation of the object provided as argument to or, when \code{file} is missing, an object that can be passed as input to a \code{mapreduce}
or \code{from.dfs} call.  }

\examples{##See \code{mapreduce} for examples}
