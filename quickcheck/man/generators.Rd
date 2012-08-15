\name{tdgg.any}
\name{tdgg.character}
\name{tdgg.constant}
\name{tdgg.data.frame}
\name{tdgg.distribution}
\name{tdgg.double}
\name{tdgg.fixed.list}
\name{tdgg.integer}
\name{tdgg.list}
\name{tdgg.logical}
\name{tdgg.mixture}
\name{tdgg.numeric.list}
\name{tdgg.prototype}
\name{tdgg.prototype.list}
\name{tdgg.raw}
\name{tdgg.select}
\name{tdgg.vector}
\alias{tdgg.any}
\alias{tdgg.character}
\alias{tdgg.constant}
\alias{tdgg.data.frame}
\alias{tdgg.distribution}
\alias{tdgg.double}
\alias{tdgg.fixed.list}
\alias{tdgg.integer}
\alias{tdgg.list}
\alias{tdgg.logical}
\alias{tdgg.mixture}
\alias{tdgg.numeric.list}
\alias{tdgg.prototype}
\alias{tdgg.prototype.list}
\alias{tdgg.raw}
\alias{tdgg.select}
\alias{tdgg.vector}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Meta-generators for random data of all types
}
\description{
These function return random data generators, argument-less functions that generate all sort of random data, to be used for the \code{generators} argument to the \code{\link{unit.test}} function.
}
\usage{
tdgg.any(p.true = 0.5, lambda.int = 100, min = -1, max = 1, len.char = 8, len.raw = 8, lambda.list = 10, list.tdg = tdgg.any(), lambda.vector = 10, max.level = 20, vector.tdg = tdgg.double())
tdgg.character(p.true = 0.5, lambda.int = 100, min = -1, max = 1, len.char = 8, len.raw = 8, lambda.list = 10, list.tdg = tdgg.any(), lambda.vector = 10, max.level = 20, vector.tdg = tdgg.double())
tdgg.constant(p.true = 0.5, lambda.int = 100, min = -1, max = 1, len.char = 8, len.raw = 8, lambda.list = 10, list.tdg = tdgg.any(), lambda.vector = 10, max.level = 20, vector.tdg = tdgg.double())
tdgg.data.frame(p.true = 0.5, lambda.int = 100, min = -1, max = 1, len.char = 8, len.raw = 8, lambda.list = 10, list.tdg = tdgg.any(), lambda.vector = 10, max.level = 20, vector.tdg = tdgg.double())
tdgg.distribution(p.true = 0.5, lambda.int = 100, min = -1, max = 1, len.char = 8, len.raw = 8, lambda.list = 10, list.tdg = tdgg.any(), lambda.vector = 10, max.level = 20, vector.tdg = tdgg.double())
tdgg.double(p.true = 0.5, lambda.int = 100, min = -1, max = 1, len.char = 8, len.raw = 8, lambda.list = 10, list.tdg = tdgg.any(), lambda.vector = 10, max.level = 20, vector.tdg = tdgg.double())
tdgg.fixed.list(p.true = 0.5, lambda.int = 100, min = -1, max = 1, len.char = 8, len.raw = 8, lambda.list = 10, list.tdg = tdgg.any(), lambda.vector = 10, max.level = 20, vector.tdg = tdgg.double())
tdgg.integer(p.true = 0.5, lambda.int = 100, min = -1, max = 1, len.char = 8, len.raw = 8, lambda.list = 10, list.tdg = tdgg.any(), lambda.vector = 10, max.level = 20, vector.tdg = tdgg.double())
tdgg.list(p.true = 0.5, lambda.int = 100, min = -1, max = 1, len.char = 8, len.raw = 8, lambda.list = 10, list.tdg = tdgg.any(), lambda.vector = 10, max.level = 20, vector.tdg = tdgg.double())
tdgg.logical(p.true = 0.5, lambda.int = 100, min = -1, max = 1, len.char = 8, len.raw = 8, lambda.list = 10, list.tdg = tdgg.any(), lambda.vector = 10, max.level = 20, vector.tdg = tdgg.double())
tdgg.mixture(p.true = 0.5, lambda.int = 100, min = -1, max = 1, len.char = 8, len.raw = 8, lambda.list = 10, list.tdg = tdgg.any(), lambda.vector = 10, max.level = 20, vector.tdg = tdgg.double())
tdgg.numeric.list(p.true = 0.5, lambda.int = 100, min = -1, max = 1, len.char = 8, len.raw = 8, lambda.list = 10, list.tdg = tdgg.any(), lambda.vector = 10, max.level = 20, vector.tdg = tdgg.double())
tdgg.prototype(p.true = 0.5, lambda.int = 100, min = -1, max = 1, len.char = 8, len.raw = 8, lambda.list = 10, list.tdg = tdgg.any(), lambda.vector = 10, max.level = 20, vector.tdg = tdgg.double())
tdgg.prototype.list(p.true = 0.5, lambda.int = 100, min = -1, max = 1, len.char = 8, len.raw = 8, lambda.list = 10, list.tdg = tdgg.any(), lambda.vector = 10, max.level = 20, vector.tdg = tdgg.double())
tdgg.raw(p.true = 0.5, lambda.int = 100, min = -1, max = 1, len.char = 8, len.raw = 8, lambda.list = 10, list.tdg = tdgg.any(), lambda.vector = 10, max.level = 20, vector.tdg = tdgg.double())
tdgg.select(p.true = 0.5, lambda.int = 100, min = -1, max = 1, len.char = 8, len.raw = 8, lambda.list = 10, list.tdg = tdgg.any(), lambda.vector = 10, max.level = 20, vector.tdg = tdgg.double())
tdgg.vector(p.true = 0.5, lambda.int = 100, min = -1, max = 1, len.char = 8, len.raw = 8, lambda.list = 10, list.tdg = tdgg.any(), lambda.vector = 10, max.level = 20, vector.tdg = tdgg.double())
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{p.true}{
%%     ~~Describe \code{p.true} here~~
}
  \item{lambda.int}{
%%     ~~Describe \code{lambda.int} here~~
}
  \item{min}{
%%     ~~Describe \code{min} here~~
}
  \item{max}{
%%     ~~Describe \code{max} here~~
}
  \item{len.char}{
%%     ~~Describe \code{len.char} here~~
}
  \item{len.raw}{
%%     ~~Describe \code{len.raw} here~~
}
  \item{lambda.list}{
%%     ~~Describe \code{lambda.list} here~~
}
  \item{list.tdg}{
%%     ~~Describe \code{list.tdg} here~~
}
  \item{lambda.vector}{
%%     ~~Describe \code{lambda.vector} here~~
}
  \item{max.level}{
%%     ~~Describe \code{max.level} here~~
}
  \item{vector.tdg}{
%%     ~~Describe \code{vector.tdg} here~~
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\references{
%% ~put references to the literature/web site here ~
}
\author{
%%  ~~who you are~~
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.

## The function is currently defined as
function (p.true = 0.5, lambda.int = 100, min = -1, max = 1, 
    len.char = 8, len.raw = 8, lambda.list = 10, list.tdg = tdgg.any(), 
    lambda.vector = 10, max.level = 20, vector.tdg = tdgg.double()) 
tdgg.mixture(tdgg.logical(p.true), tdgg.integer(lambda.int), 
    tdgg.double(min, max), tdgg.character(len.char), tdgg.vector(vector.tdg, 
        lambda.vector), tdgg.list(list.tdg, lambda.list, max.level))
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
