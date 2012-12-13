\name{status}
\alias{status}
\alias{increment.counter}
\title{
Set the status and define and increment counters for a Hadoop job
}
\description{
These are Hadoop features used to monitor and debug jobs. Should be used with a grain of salt as far as their scalability.
}
\usage{
status(value)
increment.counter(group, counter, increment = 1)
}
\arguments{
  \item{value}{The new value for the status of the job}
  \item{group}{The group for the counter}
  \item{counter}{The name for the counter}
  \item{increment}{By how much to increment the counter}
}
\details{
\code{status} set the status for the current job. \code{increment.counter} increment the counter named \code{counter} in group \code{group} by \code{increment}. If the counter doesn't exist yet it is initialized to 0. Both calls work only within the map or reduce functions and under local backend just write some messages to stderr. Unfortunately there is  no API to query the value of either status or counters at this time, but you can examine them via the jobtracker web interface.
}
\value{
NULL for both.
}

\examples{
mapreduce(to.dfs(1:1000), map = function(k,v){status("mapping"); increment.counter("Calls", "Map", 1)})
}
