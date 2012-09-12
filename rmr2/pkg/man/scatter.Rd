\name{scatter}
\alias{scatter}
\alias{gather}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Functions to split a file over several parts or to merge multiple parts into one}
\description{
\code{scatter} takes in input a file and pushes it through a mapreduce jobs that writes it over a number of parts (system dependent, specifically on the number of reducers). This helps with parallelization of the next map phase. Gather does the opposite.}
\usage{
scatter(input, output = NULL, ...)
gather(input, output = NULL, ...)
}
\arguments{
  \item{input}{
The input file}
  \item{output}{
Output, defaults to the same as \code{\link{mapreduce}} output}
  \item{\dots}{Other options passed directly to mapreduce}
}

\value{
Same as for \code{\link{mapreduce}}.
}

\notes{Scatter discards keys. This is a limitation that should be addressed in the future}