\name{rhread}
\alias{rhwrite}

\title{Utility function that reads and writes files to HDFS}
\description{Utility function that reads and writes files to HDFS}

\usage{
rhwrite(object, file = hdfs.tempfile(), textoutputformat = defaulttextoutputformat)
rhread(file, textinputformat = defaulttextinputformat)
}

\arguments{
  \item{object}{An R object to write to HDFS}
  \item{file}{A file in HDFS to read/write to}
  \item{textoutputformat}{The output format of the file being written (rhwrite only)}
  \item{textinputformat}{The input format of the file being read (rhread only)}
}

\details{ }

\examples{  }
