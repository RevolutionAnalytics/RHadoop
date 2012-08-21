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
catch.out(...)  
tdgg.any(p.true = 0.5, lambda.int = 100, min = -1, max = 1, len.char = 8, len.raw = 8, lambda.list = 10, list.tdg = tdgg.any(), lambda.vector = 10, max.level = 20, 
    vector.tdg = tdgg.double())  
tdgg.character(str.lambda = 8, len.lambda = 8)  
tdgg.constant(const)  
tdgg.data.frame(row.lambda = 20, col.lambda = 5)  
tdgg.distribution(distribution, ...)  
tdgg.double(min = -1, max = 1, lambda = 8)  
tdgg.fixed.list(...)  
tdgg.integer(elem.lambda = 100, len.lambda = 8)  
tdgg.list(tdg = tdgg.any(list.tdg = tdg, lambda.list = lambda, max.level = max.level), lambda = 10, max.level = 20)  
tdgg.logical(p.true = 0.5, lambda = 8)  
tdgg.mixture(...)  
tdgg.numeric.list(lambda = 100)  
tdgg.prototype(prototype)  
tdgg.prototype.list(prototype, lambda)  
tdgg.raw(lambda = 8)  
tdgg.select(l)  }
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{p.true}{

}
  \item{lambda.int}{

}
  \item{min}{

}
  \item{max}{

}
  \item{len.char}{

}
  \item{len.raw}{

}
  \item{lambda.list}{

}
  \item{list.tdg}{

}
  \item{lambda.vector}{

}
  \item{max.level}{

}
  \item{vector.tdg}{

}
}
\details{

}
\value{
}
\references{
}
\author{

}
\note{

}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{

}
\examples{
}

\keyword{  }
\keyword{  }
