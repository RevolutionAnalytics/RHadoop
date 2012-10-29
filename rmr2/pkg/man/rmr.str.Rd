\name{rmr.str}
\alias{rmr.str}

\title{
Print a variable's content}
\description{
One way to debug mapper and reducers on the "hadoop" backend is to print diagnostic messages. This function helps you do safely so by printing a summary of the stack, the name of the variable and its value onto standard error. This means that if Hadoop is running in standalone mode the message will appear in console, otherwise in the logs.
}
\usage{
rmr.str(x)
}
\arguments{
  \item{x}{
The variable to print
  }
}
\warning{
Remember that writing in stdout from the mapper and reducer is not allowed. Any such output will be redirected to stderr and appear in console or in the logs, depending on hadoop configuration.
}
\value{
NULL
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\examples{
mapreduce(to.dfs(1), map = function(k, v) rmr.str(v))
}
