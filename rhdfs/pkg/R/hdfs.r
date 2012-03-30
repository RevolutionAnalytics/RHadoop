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

 
hdfs.init <- function(hadoop=NULL,conf=NULL,libs=NULL,contrib=NULL,verbose=FALSE){
  if(is.null(hadoop)) hadoop <- Sys.getenv("HADOOP_HOME")
  if(is.null(conf)) conf <- Sys.getenv("HADOOP_CONF") #sprintf(Sys.getenv,hadoop)
  if(hadoop=="" || conf=="" )
    stop(sprintf("One or both of HADOOP_HOME(%s) or HADOOP_CONF(%s) is/are missing. Please set them and rerun",
                 Sys.getenv("HADOOP_HOME"),Sys.getenv("HADOOP_CONF")))
  if(is.null(libs)) libs <- sprintf("%s/lib",hadoop)
  if(is.null(contrib)) contrib <- sprintf("%s/contrib",hadoop)
  if(verbose) cat(sprintf("Detected hadoop=%s conf=%s libs=%s and contrib=%s\n",hadoop,conf,libs,contrib))
  hadoop.CP <- c(list.files(hadoop,full.names=TRUE,pattern="jar$",recursive=FALSE)
                 ,list.files(contrib,full.names=TRUE,pattern="jar$",recursive=FALSE)
                 ,list.files(libs,full.names=TRUE,pattern="jar$",recursive=FALSE)
                 ,conf
                 ,list.files(paste(system.file(package="rhdfs"),"java",sep=.Platform$file.sep),pattern="jar$",full=T)
               )
  assign("classpath",hadoop.CP,envir=.hdfsEnv)
  .jinit(classpath= hadoop.CP)
  configuration <- .jnew("org/apache/hadoop/conf/Configuration")
  cl <- .jclassLoader()
  configuration$setClassLoader(cl)
  assign("conf",configuration,envir=rhdfs:::.hdfsEnv)
  dfs <- J("org.apache.hadoop.fs.FileSystem")$get(configuration)
  assign("fs",dfs,envir=rhdfs:::.hdfsEnv)
  assign("blocksize",dfs$getDefaultBlockSize(),rhdfs:::.hdfsEnv)
  assign("replication",dfs$getDefaultReplication(),rhdfs:::.hdfsEnv)
  assign("local",J("org.apache.hadoop.fs.FileSystem")$getLocal(configuration),envir=rhdfs:::.hdfsEnv)
  assign("fu",J("com.revolutionanalytics.hadoop.hdfs.FileUtils"),envir=rhdfs:::.hdfsEnv)
}

"[[.jobjRef" <- function(x,namex,...){
  x$get(as.character(namex))
}

hdfs.defaults <- function(arg){
  if(missing(arg)){
   as.list(.hdfsEnv)
  } else .hdfsEnv[[arg]]
}
.setDefaults <- function(...){
 args <- list(...)
}
## TODO:
## 1. Convert string time to POSIXct or something
hdfs.ls <- function(path, recurse=FALSE,cfg=hdfs.defaults("conf"),fs=hdfs.defaults("fs")){
  p <- as.integer(recurse)
  v <- J("com.revolutionanalytics.hadoop.hdfs.FileUtils")$ls(fs,.jarray( as.character(path)), p )
  f <- as.data.frame(do.call("rbind", sapply(v, strsplit, "\t")), 
                     stringsAsFactors = F)
  if(nrow(f)==0) return(NULL)
  rownames(f) <- NULL
  colnames(f) <- c("permission", "owner", "group", "size", 
                   "modtime", "file")
  f$size <- as.numeric(f$size)
  unique(f)
}
hdfslist.files <- hdfs.ls

hdfs.delete <- function(path, cfg=hdfs.defaults("conf"),fs=hdfs.defaults("fs")){
  v <- rhdfs:::.hdfsEnv[["fu"]]$delete(cfg,fs,.jarray(as.character(path)), TRUE)
  TRUE
}
hdfs.rm <- hdfs.delete
hdfs.del <- hdfs.delete
hdfs.rmr <- hdfs.delete

hdfs.dircreate <- function(paths,fs=hdfs.defaults("fs")){
  v <- rhdfs:::.hdfsEnv[["fu"]]$makeFolder(fs,.jarray(as.character(paths)))
  TRUE
}
hdfs.mkdir <- hdfs.dircreate


hdfs.chmod <- function(paths,permissions="777",fs=hdfs.defaults("fs")){
  permissions <- as.character(rep(permissions,length(paths))[1:length(paths)])
  v <- rhdfs:::.hdfsEnv[["fu"]]$setPermissions(fs,.jarray(as.character(paths)), .jarray(permissions))
}

hdfs.chown <- function(paths,owner="",group="",fs=hdfs.defaults("fs")){
  owner <- as.character(rep(owner,length(paths))[1:length(paths)])
  group <- as.character(rep(group,length(paths))[1:length(paths)])
  v <- rhdfs:::.hdfsEnv[["fu"]]$setOwner(fs,.jarray(as.character(paths)), .jarray(owner),.jarray(group))
}


hdfs.file.info <- function(path,fs=hdfs.defaults("fs")){
  r <- fs$getFileStatus(.jnew("org/apache/hadoop/fs/Path",path))
  modtime <- r$getModificationTime()+ISOdate(1970,1,1)
  owner <- r$getOwner()
  group <- r$getGroup()
  size <- r$getLen()
  isDir <- r$isDir()
  block <- r$getBlockSize()
  replication <- r$getReplication()
  perms <- r$getPermission()$toString()
  data.frame(perms,isDir,block,replication,owner,group,size,modtime,path)
}

hdfs.stat <- hdfs.file.info

hdfs.exists <- function(path, fs=hdfs.defaults("fs")){
  fs$exists(.jnew("org.apache.hadoop.fs.Path",path[1]))
}

.hdfsCopy <- function(src,dest,srcFS=hdfs.defaults("fs"),dstFS=hdfs.defaults("fs"),deleteSource,overwrite,cfg=hdfs.defaults("conf")){
  if(length(dest)!=1) stop(sprintf("Destination must be a scalar"))
  if(length(src)>1 && !hdfs.file.info(dest,dstFS)$isDir){
    stop(sprintf("When copying multiple source files, destination %s must be a folder", dest))
  }
  srcp <- rhdfs:::.hdfsEnv[["fu"]]$makePathsFromStrings(.jarray(as.character(src)))
  destp <- .jnew("org.apache.hadoop.fs.Path",dest)
  J("org.apache.hadoop.fs.FileUtil")$copy(srcFS,srcp,dstFS, destp,deleteSource,overwrite,cfg)
  TRUE
}

hdfs.copy <- function(src,dest,overwrite=FALSE,srcFS=hdfs.defaults("fs"),dstFS=hdfs.defaults("fs")){
  .hdfsCopy(src=src,dest=dest,srcFS=srcFS,dstFS=dstFS,deleteSource=FALSE,overwrite=as.logical(overwrite))
}


hdfs.cp <- hdfs.copy

hdfs.move <- function(src,dest,srcFS=hdfs.defaults("fs"),dstFS=hdfs.defaults("fs")){
  .hdfsCopy(src=src,dest=dest,srcFS=srcFS,dstFS=dstFS,deleteSource=TRUE,overwrite=TRUE)
}

hdfs.mv <- hdfs.move

hdfs.rename <- function(src,dest){
  hdfs.move(src[1],dest[1],srcFS=hdfs.defaults("fs"),dstFS=hdfs.defaults("fs"))
}
hdfs.put <- function(src,dest,dstFS=hdfs.defaults("fs")){
  hdfs.copy(src,dest,overwrite=TRUE,srcFS=hdfs.defaults("local"),dstFS=dstFS)
}
hdfs.get <- function(src,dest,srcFS=hdfs.defaults("fs")){
  hdfs.copy(src,dest,overwrite=TRUE,srcFS=srcFS,dstFS=hdfs.defaults("local"))
}


hdfs.file <- function(path,mode="r",fs=hdfs.defaults("fs"),buffersize=5242880,overwrite=TRUE
                     ,replication=hdfs.defaults("replication"),blocksize=hdfs.defaults("blocksize")){
  fh <- if(mode=="r") {
    fs$open(.jnew("org.apache.hadoop.fs.Path",path[1]),as.integer(buffersize))
  }else{
    fs$create(.jnew("org.apache.hadoop.fs.Path",path[1]),
              as.logical(overwrite),as.integer(buffersize),.jshort(replication),.jlong(blocksize))
  }
  wrapp <- new.env()
  wrapp[["fh"]]     <- fh
  wrapp$mode        <- mode
  wrapp$fs          <- fs
  wrapp$buffersize  <- buffersize
  wrapp$replication <- replication
  wrapp$blocksize   <- blocksize
  wrapp$name        <- path
  class(wrapp)      <- "hdfsFH"
  reg.finalizer(wrapp, function(h) {
    hdfs.close(h)
    warning(sprintf("Closed unused DFS stream: %s",h$name))
  }, onexit = TRUE)
  wrapp
}
hdfs.close <- function(con){
  fh <- con$fh
  if(con$mode=="w") {
    fh$flush() ## change to hflush and hsync for 0.21
    fh$sync()
  }
  fh$close()
  TRUE
}
print.hdfsFH <- function(r,...){
  cat(sprintf("%s\n",as.character.hdfsFH(r)))
}
as.character.hdfsFH <- function(s){
  sprintf("DFS File: %s [blocksize=%s, replication=%s, buffersize=%s, mode='%s']\n",
          s[["name"]], s[["blocksize"]],s[["replication"]],s[["buffersize"]],s[["mode"]])
}
hdfs.write <- function(object,con,hsync=FALSE){
  obj <- switch(typeof(object),
                "raw"=object,
                serialize(object,NULL))
  fh <- con$fh
  fh$write(.jarray(obj)) 
  if(hsync) fh$hsync()
  TRUE
}
hdfs.flush <- function(con){
  con$fh$flush() ## make hlfush
  TRUE
}

hdfs.read <- function(con, n,start){
  if(missing(n)) n <- con$buffersize
  fh <- con$fh
  mu <- raw(n)
  muj <- .jarray(mu)
  if(missing(start)){
    byred <- tryCatch(fh$read(muj),EOFException=function(e){
    })
  }else{
    byred <- tryCatch(fh$read(.jlong(start),muj,0L,as.integer(n)),EOFException=function(e){
    })
  }
  if(byred<=0) return(NULL)
  bt <- .jevalArray(muj)
  bt[1:byred]
}

hdfs.seek <- function(con,n=0){
  con$fh$seek(.jlong(n))
  TRUE
}
hdfs.tell <- function(con){
  con$fh$getPos()
}

hdfs.line.reader <- function(path,n=1000L,buffer=as.integer(5*1024^2),fs=hdfs.defaults("fs")){
  n <- as.integer(n)
  f <- .jnew("com.revolutionanalytics.hadoop.hdfs.HDFSTextReader")
  f$initialize(fs,path,as.integer(buffer),as.integer(n))
  list(
       read=function(nl=n){
         .jcall(f,"[Ljava/lang/String;","getLines", as.integer(nl))
       },
       close=function(){
         .jcall(f,"V","close")
       }
       )
}

hdfs.read.text.file <- function(path, ...){
  m <- hdfs.line.reader(path, ...)
  collector <- NULL
  while(length(lines <- m$read()) > 0)
    collector <- c(collector, lines)
  m$close()
  return(collector)
}

hdfs.cat <- hdfs.read.text.file

