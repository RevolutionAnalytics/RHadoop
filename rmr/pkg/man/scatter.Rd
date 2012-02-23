\name{scatter}
\alias{scatter}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Functions to split a file over several parts}
\description{
Takes in input a file and pushes it through a mapreduce jobs that writes it over a number of parts (system dependent, specifically on the number of reducers). This helps
with parallelization of the next map phase}
\usage{
scatter(input, output = NULL)
}
\arguments{
  \item{input}{
The input file}
  \item{output}{
Output, defaults to the same as \code{\link{mapreduce}} output}
}

\value{
Same as for \code{\link{mapreduce}}.
}
