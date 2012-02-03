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
make.input.format(format = native.input.format, mode = c("binary", "text"), streaming.format = NULL, ...)
make.output.format(format = native.output.format, mode = c("binary", "text"), streaming.format = "org.apache.hadoop.mapred.SequenceFileOutputFormat", ...)}

\arguments{
  \item{format}{
  Either a string describing a predefined combination of IO settings (possibilites include: "text", "json", "csv", "native", "native.text",
               "sequence.typedbytes", "raw.typedbytes") or a function.  The exact signature and return value for this function depend on the IO direction and mode.
\preformatted{
Arguments:              Return value:
       input output             input     output
text   line  k, v       text    kv       line  
binary con   k, v, con  binary  kv       TRUE}

Where k, v are R objects playing the role of key and value, resp., con is a connection, kv is a key-value pair and line is a string.
}
  \item{mode}{
Mode can be either "text" or "binary", which tells R what type of connection to use when opening the IO connections.
}
  \item{streaming.format}{
Class to pass to hadoop streaming as inputformat or outputformat option. This class is the first in the input chain to perform its duties on the input side and the last on the output side. Right now this option is not honored in local mode.
}
\item{\dots}{
Additional arguments to the format function, for instance for the csv format they detail the specifics of the csv dialect to use, see read.table and write.table for details
}
}
\details{
The goal of these function is to encapsulate some of the complexity of the IO settings, providing meaningful defaults and predefined combinations. The input processing is the result of the composition of a Java class and an R function, and the same is true on the ouput side but in reverse order. If you don't want to deal with the full complexity of defining custom IO formats, there are pre-packaged combinations. "Text" is free text, useful mostly on the input side for NLP type applications; "json" is one or two tab separated JSON objects per record; "csv" is csv format, configurable through additional arguments; "native.text" uses the internal R serialization in text mode, and was the default, use only for backward compatibility; "native" uses the internal R serialization and is the default; "sequence.typedbytes" is a sequence file (in the Hadoop sense) where key and value are of type typedbytes, which is a simple serialization format used in connection with streaming for compatibility with other hadoop subsystems; "raw.typedbytes" is a sequence  of typedbytes objects, alternating key and value. Typedbytes is documented here \url{https://hadoop.apache.org/mapreduce/docs/current/api/org/apache/hadoop/typedbytes/package-summary.html}
}
\value{
Return a list of IO specifications, to be passed as \code{input.format} and \code{output.format} to \code{\link{mapreduce}}, and as \code{format} to  \code{\link{from.dfs}} and  \code{\link{to.dfs}}. \code{from.dfs} accepts an input format and \code{to.dfs} accepts an output format.
}
\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.

}