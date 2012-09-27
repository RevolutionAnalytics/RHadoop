\name{rmr.sample}
\alias{rmr.sample}

\title{Sample large data sets}

\description{Sample large data sets}

\usage{rmr.sample(input, output = NULL, method = c("any", "Bernoulli"), ...)}

\arguments{
  \item{input}{The data set to be sampled as a file path or  \code{\link{mapreduce}} return value}
  \item{output}{Where to store the result. See \code{\link{mapreduce}}, output argument, for details}
  \item{method}{One of "any" or "Bernoulli". "any" will return some records out, optimized for speed, but with no statistical guarantees. "Bernoulli" implements independent sampling according to the Bernoulli distribution}
  \item{\dots}{Additional arguments to fully specify the sample, they depend on the method selected. If it is "any" then the size of the desired sample should be provided as the argument \code{n}. If it is "Bernoulli" the argument \code{p} specifies the probabity of picking each record}}

\value{
The sampled data. See \code{\link{mapreduce}} for details.}

