\name{rhread}
\alias{rhread}
\alias{rhwrite}

\title{Read and write R lists to HDFS}
\description{Utility function that reads and writes lists to HDFS}

\usage{
rhwrite(object, file = hdfs.tempfile(), textoutputformat = defaulttextoutputformat)
rhread(file, textinputformat = defaulttextinputformat)
}

\arguments{
  \item{object}{An list of key-value pairs as returned by keyvalue to write to HDFS}
  \item{file}{A file in HDFS to read from or write to; for rhread, the return value of revoMapReduce}
  \item{textoutputformat}{The output format of the file being written (rhwrite only)}
  \item{textinputformat}{The input format of the file being read (rhread only)}
}

\details{ 
These functions allow to move data from RAM to HDFS and back. Keep in mind that the capacity of these two storage media is different by two or three orders of magnitude, roughly speaking, so the conversion will make sense only in specific situations. These functions do not perform any size control, so the responsibility is on the user. Rhread returns the object whose representation is contained in file. Rhwrite returns the file it wrote a representation of the object provided as argument or, when file is missing, an object that can be passed as input to a revoMapReduce call.
}

\examples{##See revoMapReduce for examples  }
