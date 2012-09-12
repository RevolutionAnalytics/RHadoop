\name{equijoin}

\alias{equijoin}

\title{
Equijoins using map reduce
}

\description{
A generalized form of equijoin, hybrid between the SQL brethen and mapreduce
}

\usage{equijoin(left.input = NULL, right.input = NULL, input = NULL,
                 output = NULL, outer = c("", "left", "right", "full"),
                 map.left = to.map(identity), map.right =
                 to.map(identity), reduce = function(k, values.left,
                 values.right) keyval(k, merge(values.left,
                 values.right, by = NULL)), reduce.all = function(k,
                 vl, vr) keyval(k, list(left = vl, right = vr)))}
\arguments{\item{left.input}{The left side input to the join.}
  \item{right.input}{The right side input to the join.}
  \item{input}{The only input in case of a self join. Mutually exclusive with the previous two.}
  \item{output}{Where to write the output.}
  \item{outer}{Whether to perform an outer join, one of the usual three types, left, right or full.}
  \item{map.left}{Function to apply to each record from the left input, follows same conventions as any map function. The returned keys
will become join keys.}
  \item{map.right}{Function to apply to each record from the right input, follows same conventions as any map function. The returned keys
will become join keys.}
  \item{reduce}{Function to be applied, key by key, on the values associated with that key. Those values are in the arguments \code{values.left} \code{values.right} and their type is determined by the type returned by the map functions, separately for the left side and the right side. This is a mapreduce like API, where you have explicit access to the grouped values and can do whatever you want with them. The default is appropriate for data frames and is equivalent to a cartesian product. The allowable return values are like those of any reduce function, see \code{\link{mapreduce}}. Specify either this or the next argument.}
  \item{reduce.all}{Function to be applied to each triple comprising a key, a value associated with that key from the left input and a
value associated with that key from the right input. This is more an SQL like interface, like an equijoin, whereby the user code has access to individual values associated with a key instead of a complete group. The allowable return values are like those of any reduce function, see \code{\link{mapreduce}}. Specify either this or the previous argument.}
}

\value{If output is specified, returns output itself. Otherwise, a \code{\link{big.data.object}}}


\section{Warning}{Doesn't work with multiple inputs like \code{mapreduce}}


\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.
 from.dfs(equijoin(left.input = to.dfs(keyval(1:10, 1:10^2)), right.input = to.dfs(keyval(1:10, 1:10^3))))
}