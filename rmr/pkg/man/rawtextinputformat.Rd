\name{raw.text.input.format}
\alias{raw.text.input.format}
\alias{raw.text.output.format}
\alias{csv.text.input.format}
\alias{csv.text.output.format}
\alias{json.text.input.format}
\alias{json.text.output.format}
\alias{native.text.input.format}
\alias{native.text.output.format}

\title{Alternative formats for text files}

\description{These functions can only be used as arguments to \code{mapreduce}, \code{from.dfs} and \code{to.dfs} when reading or writing
text files}

\usage{
  raw.text.input.format(line) 
  raw.text.output.format(k, v)
  csv.text.input.format(key = 1, ...)
  csv.text.output.format(...)
  json.text.input.format(line)
  json.text.output.format(k, v)
  native.text.input.format(line)
  native.text.output.format(k,v)
}

\arguments{
  \item{line}{The line of text to parse}
  \item{k}{A key in the mapreduce sense}
  \item{v}{A value in the mapreduce sense}
  \item{key}{Which element to use as key}
  \item{...}{Options to control the csv format details}
}

\details{Any function that reads a line of text and outputs a keyval pair is a suitable input format function; any function that accepts a
key and a value and returns a line of text is a suitable output format function. \code{raw.text.input.format} and \code{raw.text.output.format}
are examples of such functions for the raw text format (a text file without additional structure besides lines). \code{csv.text.input.format}
and \code{csv.text.output.format} instead are higher order functions that generate input and output format functions resp. For the former, the
options are the ones accepted by \code{read.table} with \code{header} defaulting to FALSE, since we are parsing a single line of a csv file
here, not a complete one. For the latter, the options are the ones accepted by \code{write.table} with \code{row.names = FALSE, col.names =
FALSE} by default. Again remember that this is a line by line writer, not a complete file writer like
\code{write.table}. \code{json.text.input.format} and \code{json.text.output.format} use two JSON objects per line, separated by a tab. Support
for R types can be tested calling \code{fromJSON(toJSON(x))} for some representative \code{x}. \code{native.text.input.format} and
\code{native.text.output.format} use an ASCII variation of the native R serde and should support any R object. It is also the
default. Additional flexibility is offered by the \code{inputformat} and \code{outputformat} arguments to \code{mapreduce}}

\examples{
\dontrun{mapreduce(input = "/somepath/somefile.txt", .text.input.format = raw.text.input.format)}}