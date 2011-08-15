\name{rawtextinputformat}
\alias{rawtextinputformat}

\title{Format specification for text files}
\description{This function can only be used as an argument to revoMapReduce when reading text files}

\usage{
rawtextinputformat(line) 
}

\arguments{
  \item{line}{The line of text to read from stdin}
}

\details{The only appropriate use of this function is as the textinputformat argument to revoMapReduce when reading plain text files. It also serves as a model for users interested in defining new text input formats. Any functions that reads a line of text and outputs a keyval pair is a suitable format specification.}

\examples{
\dontrun{revoMapReduce(input = "/somepath/somefile.txt", textinputformat = rawtextinputformat)}}