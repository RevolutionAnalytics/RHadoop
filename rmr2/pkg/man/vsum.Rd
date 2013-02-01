\name{vsum}
\alias{vsum}
\title{
Fast small sums
}
\description{
Returns the sum of a list of numeric vectors}
\usage{
vsum(x)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{
}
}
\details{
Equivalent to \code{sapply(x, sum)}, it's about 30X faster. It's often useful in reducers in the vectorized form.
}
\value{
A numeric vector with the sum of each element of the list provided as argument}

\note{
Added mostly as a demo for the new vectorized reduce. See collocations.R in the examples directory.
}

\seealso{
\code{\link{mapreduce}}
}
