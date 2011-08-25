\name{rhbase-insertion}
\alias{hb.insert}
\alias{hb.get}
\alias{hb.delete}
\alias{hb.scan}
\alias{hb.insert.data.frame}
\alias{hb.get.data.frame}

\title{Functions to Modify HBase Tables}
\description{
These functions insert/delete and iterate over HBase tables
}
\usage{
hb.insert(tablename, changes,sz=hb.defaults("sz"),hbc=hb.defaults("hbc"))
hb.get(tablename, rows, colspec,sz=hb.defaults("sz"),usz=hb.defaults("usz"),hbc=hb.defaults("hbc"))
hb.delete(tablename, rows, colspec,sz=hb.defaults("sz"),hbc=hb.defaults("hbc"))
hb.scan(tablename,startrow, end=NULL,colspec,sz=hb.defaults("sz"), usz=hb.defaults("usz"),hbc=hb.defaults("hbc"))
hb.insert.data.frame(tablename, df,sz=hb.defaults("sz"),hbc=hb.defaults("hbc"))
hb.get.data.frame(tablename, start,end=NULL,columns=NULL)
}

\arguments{
\item{tablename}{Name of the table}
\item{changes}{A list of lists, see Details}
\item{sz}{The serializer to convert values into bytes. The default is
to use R's \code{serialize} functions. This function needs to match
the format stored in the table}
\item{rows}{The row key, note that row keys for data frames are the
elements of \code{rownames(dataframe)} which are strings. It is a list of row keys}
\item{colspec}{A character vector of columns required, if NULL, all
column families and labels are returned. To request column labels 'a'
and 'b' of familiy 'F', and  all of family 'G', use
\code{c('F:a','F:b','G:')}}
\item{startrow}{The first row to start iterating from}
\item{end}{The key to stop at, if NULL the iterator continues to the end. The 'end' key is not included.}
\item{df}{A data frame}
\item{start,end}{the start and end row names of the data frame to retreive}
}

\details{ 

Functions in this group are used to insert, delete and scan key-value
pairs in the HBase table. The structure of \code{changes} is a list of
lists. Each element is a list of three elements: row key, a character
vector of column familiy + column label names (column families
e.g. 'x', column-family and column label e.g. 'x:e') and the third is
a list of the values corresponding to the entries in the column
specification. See examples.

\code{hb.scan} returns an iterator that will iterate over the
values. The iterator starts from \code{startrow} and goes till the end
of the database. If \code{end} is provided the iterator will stop at
the key preceding \code{end}. 

The remaining two functions are useful for inserting data frames and
retreiving data frames. \code{hb.insert.data.frame} will insert each
row of a data frame as key-value pair. The key is the corresponding
element of \code{rownames(df)}. For this to work, the table must
already have been created, the column families being the column names
of the data frame \code{df}. 

\code{hb.get.data.frame} returns an iterator to the data frame
beginning from \code{start} and to the row preceeding \code{end} (if
NULL, to the end). Individual columns may be specified via the
\code{columns} parameter.
 }

\value{
\code{hb.insert} returns TRUE upon successful insertion.

\code{hb.get} returns a list of lists corresponding to the \code{rows} argument. Each list is of length three: the row key, the column family/label requested and the value.

\code{hb.delete} retuns TRUE if successfully deleted. 

\code{hb.insert.data.frame} returnns TRUE if successfully inserted.

\code{hb.get} returns a named list with two entries: 'get' and 'close'. If \code{r} is the returned value, then \code{r$get(batchsize=100)} returns a batch at most 100 results (the default value for \code{batchsize} is 1000). If the iterator has reached the end it returns an empty list. \code{r$close()} closes the iterator.

\code{hb.get.data.frame} returns the same object as \code{hb.get}

}

\examples{
hb.new.table('testtable','x','y','z')
hb.describe.table('testtable')
# Not required to labels to be 'family:label', can just provide 'family'
# which is equivalent to 'family:'
hb.insert("testtable",list(list("20100101",c("x:a","x:f","y","y:w"), list("James Dewey",TRUE, 187.5,189000))))
hb.insert("testtable",list(list("20100102",c("x:a"), list("James Agnew"))))
hb.insert("testtable",list(list("20100103",c("y:a","y:w"), list("Dilbert Ashford",250000))))
hb.insert("testtable",list(list("20100104",c("x:f"), list("Henry Higs"))))


hb.get("testtable",list("20100101","20100102"))
hb.get("testtable",list("20100101","20100102"),c("y")) # columns that start with y
hb.get("testtable",list("20100101","20100102"),c("y:w"))
hb.get("testtable",list("20100101","20100102"),c("y:w","z"))


# delete 
hb.delete("testtable","20100103","y:a")
hb.get("testtable","20100103")

## will not get 20100103, since no 'x'
iter <- hb.scan("testtable",startrow="20100100",colspec="x")
while( length(row <- iter$get(1))>0){
  print(row)
}
iter$close()

# scan from beginning
iter <- hb.scan("testtable",startrow="20100100",colspec=c('x','y','z'))
while( length(row <- iter$get(1))>0){
  print(row)
}
iter$close()

# scan uptil 20100103 (not including it)
iter <- hb.scan("testtable",startrow="20100100",end="20100103",colspec=c('x','y','z'))
while( length(row <- iter$get(1))>0){
  print(row)
}
iter$close()


# Insert a data frame
x <- data.frame(x=c("Winston","Jen","Jasmine"), y=runif(3),z=c(1,2,3))
rownames(x) <- c("20100105","20100106","20100107")
hb.insert.data.frame("testtable",x)

# get data frame, note the columns have ":" appended
iter <- hb.get.data.frame("testtable",start="20100105")
iter()
# Uptil penultimate row
iter <- hb.get.data.frame("testtable",start="20100105",end="20100107")
iter()
}
