# Copyright 2011 Revolution Analytics
#    
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

 
hb.defaults <- function(arg){
  if(missing(arg)){
    as.list(.hbEnv)
  } else rhbase:::.hbEnv[[arg]]
}

hb.init <- function(host='127.0.0.1', port=9090, buffsize=3*1024*1024, serialize=c("native", "raw")){
  ## initializes an hbase thrift connection
  ## host     = the hostname of the thrift server
  ## port     = the port on which the thrift server is listening
  ## buffsize = buffer size of subsequent reads
  ## serialize= the serialization method used to read and write data to hbase
  
  ## return: an object of class "hb.client.connection"
  y <- .Call("initialize",host,as.integer(port,buffsize),PACKAGE="rhbase")
  class(y) <- "hb.client.connection"
  reg.finalizer(y,function(r){
    .Call("deleteclient",r,PACKAGE="rhbase")
  })
  assign('hbc',y,envir=rhbase:::.hbEnv)
  opt.names <- list("maxversions"=as.integer,"compression"=as.character,
  "inmemory"=as.logical, "bloomfiltertype"=as.character,
  "bloomfiltervecsize"=as.integer, "bloomfilternbhashes"=as.integer,
  "blockcache"=as.logical, "timetolive"=as.integer)
  assign("opt.names",opt.names,envir=rhbase:::.hbEnv)
  
  serialize<-match.arg(serialize)
  if (serialize=="native") {
      assign("sz",function(r) serialize(r,NULL),envir=rhbase:::.hbEnv)
      assign("usz",unserialize,,envir=rhbase:::.hbEnv)
  }else if (serialize=="raw") {
      assign("sz",function(r) charToRaw(toString(r)),envir=rhbase:::.hbEnv)
      assign("usz",function(r) rawToChar(r),envir=rhbase:::.hbEnv)
  }
  y

}

hb.list.tables <- function(hbc=hb.defaults('hbc')){
  ## Provides a character vector of tables
  ## hbc = object of class "hb.client.connection" (returned from hb.init)
  ## return: a data frame of tables and their columns (as returned by hb.describe.table)
  y <- .Call("hb_get_tables",hbc,PACKAGE="rhbase");
  f <- lapply(y, hb.describe.table)
  names(f) <- y
  f
}

hb.describe.table <- function(tablename, hbc=hb.defaults('hbc')){
  y <- .Call("hbDescribeCols",hbc,as.character(tablename),PACKAGE="rhbase");
  do.call("rbind",lapply(y,function(r){
    z <- as.data.frame(r,stringsAsFactors=FALSE)
    colnames(z) <- c("maxversions","compression","inmemory","bloomfiltertype",
                     "bloomfiltervecsize","bloomfilternbhashes","blockcache","timetolive")
    z}))
}

hb.set.table.mode <- function(tablename,enable,hbc=hb.defaults('hbc')){
  ## Enables/Disables the table
  ## enable    = TRUE/FALSE  to enable or disable the table
  ## tablename = name of the table
  ## hbc       = object returned from hb.init
  ## return: the updates table status
  if(missing(enable)){
    return(.Call("hbIsTableEnabled",hbc,as.character(tablename)))
  }else{
    return(.Call("hbTableMode",hbc,as.character(tablename), as.integer(enable)))
  }
}

hb.delete.table <- function(tablename,hbc=hb.defaults("hbc")){
  hb.set.table.mode(tablename,enable=FALSE, hbc)
  .Call("hbDeleteTable",hbc,as.character(tablename))
  TRUE
}

hb.compact.table <- function(tablename, major=FALSE,hbc=hb.defaults("hbc")){
  ## Compaction of table
  ## tablename = name of the table
  ## major     = TRUE/FALSE for major or minor compaction
  ## hbc       = object returned from hb.init
  ## return: TRUE on success, error if error
 .Call("hbCompact",hbc,as.character(tablename),as.integer(major))
 TRUE
}

hb.new.table <- function(tablename,...,opts=list(),hbc=hb.defaults("hbc")){
  vars <- list(...)
  if(length(vars)==0) stop("Must provide at least one column family")
  opt.names <- hb.defaults("opt.names")
  dfo <- list()
  n <- names(opt.names)
  for(x in names(opts)){
    if(x %in% n) {
      dfo[[x]] <- opt.names[[x]](opts[[x]])
      opts[[x]] <- NULL
    }
  }
  z <- lapply(vars,function(varname){
    r <- list()
    r[[1]] <- varname
    p <- dfo
    if(!is.null(opts[[varname]])) {
      for(x in names(opts[[varname]]))
        if(x %in% n)
          p[[x]] <- opt.names[[x]](opts[[varname]][[x]])
    }
    r[[2]] <- p
    r
  })
  .Call("hbCreateTable",hbc,tablename,z)
  TRUE
}

hb.regions.table <- function(tablename, hbc=hb.defaults("hbc"),usz=hb.defaults("usz")){
   y <- .Call("hbGetRegions",hbc, tablename)
   y
   lapply(y,function(r) list(start=ifelse(is.null(r[[1]]),NA,usz(r[[1]])),
                             end=ifelse(is.null(r[[2]]),NA,usz(r[[2]])),id=r[[3]],name=rawToChar(r[[4]]),version=r[[5]]))
}

hb.insert <- function(tablename, changes,sz=hb.defaults("sz"),hbc=hb.defaults("hbc")){
  ## changes a colfam:colum to the value for rows
  ## tablename = name of a table
  ## changes = list of lists, each list a row, where row:= list(rowid, name.vector, list.of.values)
  batchmutation <- .Call("hbnewBatchMutation", length(changes));
  if(length(changes)==1){
    row=changes[[1]]
    .Call("makeAndSendOneMutation", hbc,tablename,sz(row[[1]]), #the row id
          row[[2]], #name vector
          lapply(row[[3]],sz))
  }else{
    ## print(system.time(
    lapply(changes,function(row){
      .Call("addToCurrentBatch", batchmutation,
            sz(row[[1]]), #the row id
            row[[2]], #name vector
            lapply(row[[3]],sz))
    })
                      ## ))
    ## print(system.time(
    .Call("sendCurrentBatch", hbc, tablename, batchmutation)
                      ## ))
  }
  TRUE
}

hb.get <- function(tablename, rows, colspec,sz=hb.defaults("sz"),
                   usz=hb.defaults("usz"),hbc=hb.defaults("hbc")){
  if(!is.list(rows)) rows <- as.list(rows)
  y <- if(missing(colspec)){
     .Call("hbgetRow",hbc, tablename,lapply(rows, sz))
   }else{
     .Call("hbgetRowWithColumns",hbc,tablename,lapply(rows, sz),colspec)
   }
  lapply(y,function(r) list(usz(r[[1]]),r[[2]],lapply(r[[3]],usz)))
}

 hb.delete <- function(tablename, rows, colspec,sz=hb.defaults("sz"),
                      hbc=hb.defaults("hbc")){
  ## Deletes rows from tablename
  ## tablename   = name of table
  ## rows        = list of vector of rows to delete
  ## colspec     = a vector of col:fam or just col: that will be deleted from /all/
  ## rows
  ## allversions = delete all the versions?
  ## sz          = serializer function
  ## usz         = deserializer function
  allversions <- TRUE
  if(!is.list(rows)) rows <- as.list(rows)
  y <- if(missing(colspec)){
    if(!allversions) stop("allversions=FALSE only works when colspec is provided")
     .Call("hbdeleteAllRow",hbc, tablename,lapply(rows, sz),as.logical(allversions))
   }else{
     .Call("hbdeleteAll",hbc,tablename,lapply(rows, sz),colspec,as.logical(allversions))
   }
  TRUE
}


hb.scan <- function(tablename, startrow, end=NULL, colspec,sz=hb.defaults("sz"), usz=hb.defaults("usz"),
                    hbc=hb.defaults("hbc")){
  ## Scans thr
  if(missing(colspec)) stop("Enter a colspec vector, e.g. c('x:f','x:u')")
  if(!is.null(end)){
    scanner <- .Call("hbScannerOpenFilter", hbc, tablename, sz(startrow), sz(end), colspec,as.integer(0))
  }else{
    scanner <- .Call("hbScannerOpenFilter", hbc, tablename, sz(startrow), "", colspec,as.integer(2))
  }
  mu <- function(batchsize=1000){
    x <- .Call("hbScannerGetList",hbc,scanner, as.integer(batchsize))
    lapply(x,function(r) list(usz(r[[1]]),r[[2]],lapply(r[[3]],usz)))
  }
  fu <- function(){
    invisible(.Call("hbCloseScanner",hbc,scanner))
  }
  return(list(get=mu,close=fu))
}



hb.insert.data.frame <- function(tablename, df,sz=hb.defaults("sz"),hbc=hb.defaults("hbc")){
  ## each rowname is the key
  ## the columnnames correspond to columns created in hb.new.table
  if(!"data.frame" %in% class(df)) stop("df must be a data frame")
  rown <- rownames(df)
  coln <- colnames(df)
  if(is.null(coln)) stop("df does not have column names")
  .Call("addAndSendDFBatch" ,hbc,tablename,lapply(rown,sz), coln ,lapply(1:nrow(df),function(r) lapply(as.list(df[r,]),sz)))
  TRUE
}

hb.get.data.frame <- function(tablename, start,end=NULL,columns=NULL){
  if(is.null(columns))
    columns <- rownames(hb.describe.table(tablename))
  if(is.null(end))
    scn <- hb.scan(tablename, startrow=start, colspec=columns)
  else
    scn <- hb.scan(tablename, startrow=start,end=end,colspec=columns) ## filter is not included
  function(batchsize=100){
    f <- scn$get(batchsize)
    if(length(f)==0) {
      scn$close()
      return(NULL)
    }
    g <- as.data.frame(    lapply(1:length(columns),function(cc) unlist(lapply( lapply(f,"[[",3),"[[",cc)))    )
    rownames(g) <- unlist(lapply(f,"[[",1))
    colnames(g) <- columns
    g
  }
}

