\name{rhread}
\alias{rhread}
\alias{rhwrite}

\title{Read and write R lists to HDFS}
\description{Utility function that reads and writes lists to HDFS}

\usage{
rhwrite(object, file = hdfs.tempfile(), textoutputformat = defaulttextoutputformat)
rhread(file, textinputformat = defaulttextinputformat, todataframe = F)
}

\arguments{
  \item{object}{An list of key-value pairs as returned by keyvalue to write to HDFS; experimentally, a data frame}
  \item{file}{A file in HDFS to read from or write to; for rhread, the return value of revoMapReduce}
  \item{textoutputformat}{The output format of the file being written (rhwrite only)}
  \item{textinputformat}{The input format of the file being read (rhread only)}
  \item{todataframe}{Do what it takes to return a data frame (rhread only). }
}



\details{ 
These functions allow to move data from RAM to HDFS and back. Keep in mind that the capacity of these two storage media is different by two or three orders of magnitude, roughly speaking, so the conversion will make sense only in specific situations. These functions do not perform any size control, so the responsibility is on the user.}

\value{\code{rhread} returns the object whose representation is contained in \code{file}. \code{rhwrite} returns the file it wrote a representation of the object provided as argument to or, when {file} is missing, an object that can be passed as input to a \code{revoMapReduce} or \code{rhread} call.
}

\examples{##See revoMapReduce for examples  }
