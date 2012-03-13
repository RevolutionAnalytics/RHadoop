\name{rhbase-admin}
\alias{hb.defaults}
\alias{hb.init}
\alias{hb.defaults}
\alias{hb.init}
\alias{hb.list.tables}
\alias{hb.describe.table}
\alias{hb.set.table.mode}
\alias{hb.delete.table}
\alias{hb.new.table}
\alias{hb.regions.table}
\alias{hb.compact.table}


\title{Initializing the HBase package and Managing Tables}
\description{
   These functions initialize the package and store some read-only defaults. Also, 
   some of these functions manage (that is, create or describe) tables in HBase.
}

\usage{
hb.defaults(arg)
hb.init(host='127.0.0.1', port=9090, buffsize=3*1024*1024, serialize=c("native", "raw"))
hb.list.tables(hbc=hb.defaults('hbc'))
hb.describe.table(tablename, hbc=hb.defaults('hbc'))
hb.set.table.mode(tablename,enable,hbc=hb.defaults('hbc'))
hb.delete.table(tablename,hbc=hb.defaults("hbc"))
hb.new.table(tablename,...,opts=list(),hbc=hb.defaults("hbc"))
hb.regions.table(tablename, hbc=hb.defaults("hbc"),usz=hb.defaults("usz"))
hb.compact.table(tablename, major=FALSE,hbc=hb.defaults("hbc"))
}

\arguments{
\item{arg}{If empty, returns a list of options. These are read
only. The options are "usz", the function to unserialize data. "sz" is
the function to serialize data. "hbc" is a handle to the client
connection. "opt.names" is a list of transformer functions and used
internally.}

\item{host,port}{The host name and port number on which the HBase Thrift server is running,
see details}
\item{buffsize}{The size of the transfer buffer}
\item{serialize}{The default serialization scheme. "native" [default] - R native functions serialize/unserialize for reading and 
writing to hbase tables.  "raw" - indicates raw bytes converted to string for reading and writing to hbase tables.}
\item{tablename}{The name of an HBase table}
\item{enable}{TRUE/FALSE to enable or disable the table - prevents
modifications to the table}
\item{hbc}{The client handle which should not be changed}
\item{usz}{Functions that serialize and unserisalize the objects in
the database, see Details}
\item{...}{colum names for the created table}
\item{opts}{Modifiers for the column names, see Details}
\item{major}{TRUE/FALSE for major or minor compaction}
}

\details{ 
	  The HBase Thrift server can be started (usually) as
	  \code{hbase thrift start -b 127.0.0.1 -p 9090}, which starts
	  an HBase Thrift server on localhost listening on port 9090.

	  The \code{sz,usz} parameters are used to serialize to bytes
	    and deserialize the keys and vectors from HBase. The
	    default is R's \code{serialize} and
	    \code{unserialize}. Note, HBase has no record of how the
	    data was serialized so it is imperative that the relevant
	    serializer and deserializer functions are used.

	    \code{opts} are modifiers for the columns. It can be
	    specified much like the \code{scale} parameter in
	    \code{xyplot}. The names of the list are :
            \itemize{
	     \item{maxversions}{The number of versions of the column,
	    default is 3}
	    \item{compression}{The type of compression, allowed values
	    are 'GZ','LZO' and 'None'(default)}
            \item{inmemory}{keeps data in memory using HRegionServer
	    cache, FALSE(default)/TRUE}
            \item{bloomfiltertype}{Type of bloom filter used: 'None'(default),'ROW','ROWCOL'}
	    \item{blockcache}{TRUE/FALSE(default) if block cache is
	    enabled, see
	    \url{http://www.slideshare.net/udanax/bigtable-and-hbase}}
	    \item{timetolive}{Time-to-live of cell contents, in
	    seconds, -1 means forever(default)}}
}

\value{
	\item{hb.defaults}{returns a named list of
	options.}
	\item{hb.init}{returns a client handle.}
	\item{hb.list.tables}{A named list (names are table names),
	each list entry is the return value of
	\code{hb.describe.table}}
	\item{hb.set.table.mode,hb.delete.table,hb.new.table}{TRUE if
	successful, error if something unfortunate happens}
	\item{hb.regions.table}{A list as long as then number of
	regions of the table, each list entry the start key and end key}
}

\examples{
     library(rhbase)
     hb.init()
     hb.new.table("foo4","x","y",opts=list(maxversions=5,x=list(maxversions=1L,compression='GZ',inmemory=TRUE)))
     hb.describe.table("foo4")
     hb.delete.table("foo4")
}
