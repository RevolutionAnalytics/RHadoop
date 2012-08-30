\name{make.input.format}
\alias{make.input.format}
\alias{make.output.format}

\title{
 Create combinations of settings for flexible IO
}
\description{
Create combinations of IO settings either from named formats or from a combination of a Java class, a mode and an R function
}
\usage{
make.input.format(format = make.native.input.format(), mode = c("binary", "text"), streaming.format = NULL, ...)
make.output.format(format = native.output.format, mode = c("binary", "text"), streaming.format = "org.apache.hadoop.mapred.SequenceFileOutputFormat", ...)}

\arguments{
  \item{format}{Either a string describing a predefined combination of IO settings (possibilities include: "text", "json", "csv", "native","sequence.typedbytes") or a function. For an input format, this function accepts a connection and a number of records and returns a key-value pair when the specified number of records is one and a vectorized key-value pair ohterwise (see \code{\link{keyval}}. For an output format, this function accepts a key, a value, a connection and a \code{vectorized} flag and writes the first two to the connection. When the vectorized flag is TRUE, the key and value are construed as equal length lists or vectors of keys and values, or data frames with an equal number of rows, positionally matched.}
  \item{mode}{Mode can be either "text" or "binary", which tells R what type of connection to use when opening the IO connections.}
  \item{streaming.format}{Class to pass to hadoop streaming as inputformat or outputformat option. This class is the first in the input chain to perform its duties on the input side and the last on the output side. Right now this option is not honored in local mode.}
  \item{\dots}{Additional arguments to the format function, for instance for the csv format they detail the specifics of the csv dialect to use, see \code{\link{read.table}} and \code{\link{write.table}} for details}
}
\details{
The goal of these function is to encapsulate some of the complexity of the IO settings, providing meaningful defaults and predefined combinations. The input processing is the result of the composition of a Java class and an R function, and the same is true on the output side but in reverse order. If you don't want to deal with the full complexity of defining custom IO formats, there are pre-packaged combinations. "text" is free text, useful mostly on the input side for NLP type applications; "json" is one or two tab separated, single line JSON objects per record; "csv" is the csv format, configurable through additional arguments; "native.text" uses the internal R serialization in text mode, and was the default in previous releases, use only for backward compatibility; "native" uses the internal R serialization, offers the highest level of compatibility with R data types and is the default; "sequence.typedbytes" is a sequence file (in the Hadoop sense) where key and value are of type typedbytes, which is a simple serialization format used in connection with streaming for compatibility with other hadoop subsystems. Typedbytes is documented here \url{https://hadoop.apache.org/mapreduce/docs/current/api/org/apache/hadoop/typedbytes/package-summary.html}
}
\value{
Return a list of IO specifications, to be passed as \code{input.format} and \code{output.format} to \code{\link{mapreduce}}, and as \code{format} to  \code{\link{from.dfs}} (input) and  \code{\link{to.dfs}} (output).
}
\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.

}