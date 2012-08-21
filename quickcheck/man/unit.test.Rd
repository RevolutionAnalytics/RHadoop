\name{unit.test}
\alias{unit.test}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Generate unit tests}
\description{
Evaluates a predicate with n arguments provided by evaluating the generators. Stops or returns test cases.
}
\usage{
unit.test(predicate, generators = list(), sample.size = 10, precondition = function(...) TRUE, stop = TRUE)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{predicate}{

}
  \item{generators}{

}
  \item{sample.size}{

}
  \item{precondition}{

}
}
\details{

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

}
\note{

}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{

}
\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.

## The function is currently defined as
function (predicate, generators, sample.size = 10, precondition = function(...) T) 
{
    set.seed(0)
    options(warning.length = 8125)
    results = sapply(1:sample.size, function(i) {
        args = lapply(generators, function(a) a())
        if (do.call(precondition, args) && !do.call(function(...) {
            tryCatch(predicate(...), error = function(e) {
                traceback()
                print(e)
                FALSE
            })
        }, args)) {
            print(paste("FAIL: predicate:", paste(deparse(predicate), 
                collapse = " ")))
            list(predicate = predicate, args = args)
        }
    }, simplify = TRUE)
    if (length(results) == 0) 
        print(paste("Pass ", paste(deparse(predicate), "\n", 
            collapse = " ")))
    else results
  }
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
