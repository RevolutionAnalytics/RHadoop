\name{rawtextinputformat}
\alias{rawtextinputformat}
\alias{rawtextoutputformat}
\alias{csvtextinputformat}
\alias{csvtextoutputformat}
\alias{jsontextinputformat}
\alias{jsontextoutputformat}
\alias{nativetextinputformat}
\alias{nativetextoutputformat}

\title{Alternative formats for text files}

\description{These functions can only be used as arguments to \code{mapreduce}, \code{from.dfs} and \code{to.dfs} when reading or writing
text files}

\usage{
  rawtextinputformat(line) 
  rawtextoutputformat(k, v)
  csvtextinputformat(key = 1, ...)
  csvtextoutputformat(...)
  jsontextinputformat(line)
  jsontextoutputformat(k, v)
  nativetextinputformat(line)
  nativetextoutputformat(k,v)
}

\arguments{
  \item{line}{The line of text to parse}
  \item{k}{A key in the mapreduce sense}
  \item{v}{A value in the mapreduce sense}
  \item{key}{Which element to use as key}
  \item{...}{Options to control the csv format details}
}

\details{Any function that reads a line of text and outputs a keyval pair is a suitable input format function; any function that accepts a
key and a value and returns a line of text is a suitable output format function. \code{rawtextinputformat} and \code{rawtextoutputformat}
are examples of such functions for the raw text format (a text file without additional structure besides lines). \code{csvtextinputformat}
and \code{csvtextoutputformat} instead are higher order functions that generate input and output format functions resp. For the former, the
options are the ones accepted by \code{read.table} with \code{header} defaulting to FALSE, since we are parsing a single line of a csv file
here, not a complete one. For the latter, the options are the ones accepted by \code{write.table} with \code{row.names = FALSE, col.names =
FALSE} by default. Again remember that this is a line by line writer, not a complete file writer like
\code{write.table}. \code{jsontextinputformat} and \code{jsontextoutputformat} use two JSON objects per line, separated by a tab. Support
for R types can be tested calling \code{fromJSON(toJSON(x))} for some representative \code{x}. \code{nativetextinputformat} and
\code{nativetextoutputformat} use an ASCII variation of the native R serde and should support any R object. It is also the
default. Additional flexibility is offered by the \code{inputformat} and \code{outputformat} arguments to \code{mapreduce}}

\examples{
\dontrun{mapreduce(input = "/somepath/somefile.txt", textinputformat = rawtextinputformat)}}