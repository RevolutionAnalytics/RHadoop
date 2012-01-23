\name{make.input.specs}
\alias{make.input.specs}
\alias{make.output.specs}

\title{
 Create combinations of settings for flexible IO
}
\description{
Create combinations of IO settings either from named formats or from a combination of a Java class, a mode and an R function
}
\usage{
make.input.specs(format = native.input.format, mode = c("text", "binary"), streaming.input.format = NULL, ...)
make.output.specs(format = native.output.format, mode = c("text", "binary"), streaming.output.format = NULL, ...)}

%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{format}{
  Either a string describing a predefined combination of IO settings (possibilites include: "text", "json", "csv", "native", "native.binary",
               "sequence.typedbytes", "raw.typedbytes") or a function.  The exact signature and return value for this function depend on the IO direction and mode.
\code{
Arguments:              Return value:
       input output             input     output
text   line  k, v       text    kv       line  
binary con   k, v, con  binary  kv       TRUE}

Where k, v are R objects playing the role of key and value, resp., con is a connection, kv is a key-value pair and line is a string.
}
  \item{mode}{
Mode is either "text" or "binary", which tells R what type of connection to use when opening the IO connections.
}
  \item{streaming.input.format}{
Class to pass to hadoop streaming as inputformat option. This class is the first in the input chain to perform its duties.
Right now there is not way for this option to be honored in local more nor by from.dfs and to.dfs.
}
\item{streaming.output.format}{Class to pass to hadoop streaming as outputformat option. This class is the last in the output chain to perform its duties.
Right now there is not way for this option to be honored in local more nor by from.dfs and to.dfs.
}
  \item{\dots}{
Additional argument to the format function, for instance for the csv format provides the specific of the csv dialect to use, see read.table and write.table for details
}
}
\details{
The goal of these function is to encapsulate some of the complexity of the IO settings, providing meaningful defaults and predefined combinations. The input processing is the result of the composition of a Java class and an R function, and the same is true on the ouput side but in reverse order. If you don't want to deal with the full complexity of defining custom IO formats, there are pre-packaged combinations. "Text" is free text, useful mostly on the input side for NLP type applications; "json" is a tab separated pair of JSON objects per record; "csv" is csv format, configurable through additional arguments; "native" uses the internal R serialization in text mode, and is the current default; "native.binary" uses the internal R serialization and is meant to replace the text version when more thoroughly tested; "sequence.typedbytes" is a sequence file (in the Hadoop sense) where key and value are of type typedbytes, which is a simple serialization format used in connection with streaming for compatibility with other hadoop subsystems; "raw.typedbytes" is a sequence  of typedbytes objects, alternating key and value.
}
\value{
Returns a list of IO specification, to be passed as input.specs to mapreduce, from.dfs and to.dfs
}
\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.

}