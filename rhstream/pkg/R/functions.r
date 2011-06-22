createReader = function(linebufsize = 2000, textinputformat){
  con = file("stdin", open="r")
  close = function(){
    ## close(con)
  }
  readChunk = function(){
    lines = readLines(con = con, n = linebufsize, warn = FALSE)
    if(length(lines) > 0){
      return(lapply(lines, textinputformat))
    }else{
      return(NULL) 
    }
  }
  return(list(close = close, get = readChunk))
}

send = function(out, textoutputformat = defaulttextoutputformat){
  if (is.null(attr(out, 'keyval', exact = TRUE))) 
    lapply(out, function(o) cat(textoutputformat(o$key, o$val)) )
  else
    cat(textoutputformat(out$key, out$val))
  TRUE
}

counter = function(group="r-stream",family, value){
  cat(sprintf("report:counter:%s,$s,%s",
              as.character(group),
              as.character(family),
              as.integer(value)),
      stderr())
}

status = function(what){
  cat(sprintf("report:status:%s",
              as.character(what)),
      stderr())
}

Sum = function(id,p=1){
  if(typeof(p)=='double')
    cat(sprintf("DoubleValueSum:%s\t%s\n",id,p))
  else
    cat(sprintf("LongValueSum:%s\t%s\n",id,p))
}
Min = function(id,p){
  if(typeof(p)=='double')
    cat(sprintf("DoubleValueMin:%s\t%s\n",id,p))
  else
    cat(sprintf("LongValueMin:%s\t%s\n",id,p))
}
Max = function(id,p){
  if(typeof(p)=='double')
    cat(sprintf("DoubleValueMax:%s\t%s\n",id,p))
  else
    cat(sprintf("LongValueMax:%s\t%s\n",id,p))
}
Hist = function(id,p){
  ## No. of Uniq, Min,Median,Max,Average,Std Dev
    cat(sprintf("ValueHistogram:%s\t%s\n",id,p))
}  

## could think of this as a utils section
getKeys = function(l) lapply(l, function(x) x[[1]])

getValues = function(l) lapply(l, function(x) x[[2]])

keyval = function(k, v = NULL, i = 1) {
  if(is.null(v)) {
      if(length(k) == 2){
          v = k[[2]]
          k = k[[1]]}
      else {
          v = k
          k = v[[i]]
      }
  }
  kv = list(key = k, val = v)
  attr(kv, 'keyval') = TRUE
  kv}

mkMapper = function(fun = identity, keyfun = identity, valfun = identity) {
  if (!missing(fun)) {
    function(k,v) keyval(fun(k,v))}
  else {
    function(k,v) keyval(keyfun(k), valfun(v))}}

mkReducer = mkMapper
mkMapRedFun = mkMapper

mkMapRedListFun = function(fun = identity, keyfun = identity, valfun = identity) {
  if (!missing(fun)) {
    function(k,v) lapply(fun(k,v), keyval)}
  else {
    function(k,v) lapply(as.list(izip(keyfun(k), valfun(v))), keyval)}}

## end utils

mapDriver = function(map, linebufsize, textinputformat, textoutputformat){
  k = createReader(linebufsize, textinputformat)
  while( !is.null(d <- k$get())){
    lapply(d,
           function(r) {
             out = map(r[[1]], r[[2]])
             if(!is.null(out))
               send(out, textoutputformat)
           })
  }
  k$close()
  invisible()
}

listComp = function(ll,e) sapply(ll, function(l) isTRUE(all.equal(e,l)))

reduceDriver = function(reduce, linebufsize, textinputformat, textoutputformat){
    k = createReader(linebufsize, textinputformat)
    lastKey = NULL
    lastGroup = list()
    while( !is.null(d <- k$get())){
        d = c(lastGroup,d)
        lastKey =  d[[length(d)]][[1]]
        groupKeys = getKeys(d)
        lastGroup = d[listComp(groupKeys, lastKey)]
        d = d[!listComp(groupKeys, lastKey)]
        if(length(d) > 0) {
            groups = tapply(d, sapply(getKeys(d), digest), identity, simplify = FALSE)
            lapply(groups,
                   function(g) {
                       out = reduce(g[[1]][[1]], getValues(g))
                       if(!is.null(out))
                           send(out, textoutputformat)
                   })
        }
    }
    out = reduce(lastKey, getValues(lastGroup))
    send(out, textoutputformat)
    k$close()
    invisible()
}


make.job.conf = function(m,pfx){
  N = names(m)
  if(length(m) == 0) return(" ")
  paste(unlist(lapply(1:length(N),
                      function(i){
                        sprintf("%s %s=%s ", pfx, N[[i]],as.character(m[[i]]))})),
        collapse = " ")}

make.cache.files = function(caches,pfx,shorten = TRUE){
  if(length(caches) == 0) return(" ")
  sprintf("%s %s",pfx, paste(sapply(caches,
                                    function(r){
                                      if(shorten) {
                                        path.leaf = tail(strsplit(r,"/")[[1]],1)
                                        sprintf("'%s#%s'",r,path.leaf)
                                      }else{
                                        sprintf("'%s'",r)}}),
                             collapse = ","))}

make.input.files = function(infiles){
  if(length(infiles) == 0) return(" ")
  paste(sapply(infiles,
               function(r){
                 sprintf("-input %s ", r)}),
        collapse=" ")}

defaulttextinputformat = function(line) {
  x =  strsplit(line, "\t")[[1]]
  keyval(fromJSON(x[1]), fromJSON(x[2]))}

defaulttextoutputformat = function(k,v) {
    paste(toJSON(k), "\t", toJSON(v), "\n", sep = "")}

rhwrite = function(object, file, textoutputformat = defaulttextoutputformat){
    con = hdfs.file(paste(file, "part-00000", sep = "/"), 'w')
    hdfs.write(
               object =
               charToRaw
               (paste
                (lapply
                 (object,
                  function(x) {kv = keyval(x)
                               textoutputformat(kv$key, kv$val)}),
                 collapse="")),
               con=con)
    hdfs.close(con)
}

rhread = function(file, textinputformat = defaulttextinputformat){
    do.call(c, lapply(hdfs.ls(path = file)$file, function(f) lapply(hdfs.read.text.file(f), textinputformat)))}

## The function to run a map and reduce over a hadoop cluster does not implement
## a general combine unless it is one of the streaminga default combine.

revoMapReduce = function(
  map,
  reduce,
  infile,
  outfile,
  verbose = FALSE,
  inputformat = NULL,
  textinputformat = defaulttextinputformat,
  textoutputformat = defaulttextoutputformat,
  debug = FALSE) {
    rhstream(map = map,
             reduce = reduce,
             in.folder = infile,
             out.folder = outfile,
             verbose = verbose,
             inputformat = inputformat,
             textinputformat = textinputformat,
             textoutputformat = textoutputformat)
}

rhstream = function(
  map,
  reduce,
  in.folder,
  out.folder, 
  linebufsize = 2000,
  verbose = FALSE,
  numreduces,
  cachefiles = c(),
  archives = c(),
  jarfiles = c(),
  otherparams = list(),
  mapred = list(),
  mpr.out = NULL,
  inputformat = NULL,
  textinputformat = defaulttextinputformat,
  textoutputformat = defaulttextoutputformat,
  debug = FALSE) {
  ## prepare map and reduce executables
  lines = '#! /usr/bin/env Rscript
options(warn=-1)

library(RevoHStream)
load("RevoHStreamParentEnv")
load("RevoHStreamLocalEnv")
'
  mapLine = 'RevoHStream:::mapDriver(map = map,
              linebufsize = linebufsize,
              textinputformat = textinputformat,
              textoutputformat = if(missing(reduce))
                                 {textoutputformat}
                                 else {RevoHStream:::defaulttextoutputformat})'
  reduceLine  =  'RevoHStream:::reduceDriver(reduce = reduce,
                 linebufsize = linebufsize,
                 textinputformat = RevoHStream:::defaulttextinputformat,
                 textoutputformat = textoutputformat)'
  
  map.file = tempfile(pattern="rhstr.map")
  writeLines(c(lines,mapLine), con = map.file)
  reduce.file = tempfile(pattern = "rhstr.reduce")
  writeLines(c(lines, reduceLine), con = reduce.file)

  # set up the execution environment for map and reduce
  save.image(file="RevoHStreamParentEnv")
  save(list = ls(all = TRUE, envir = environment()), file = "RevoHStreamLocalEnv", envir = environment())
  image.cmd.line = "-file RevoHStreamParentEnv -file RevoHStreamLocalEnv"

  ## prepare hadoop streaming command
  hadoopHome = Sys.getenv("HADOOP_HOME")
  if(hadoopHome == "") warning("Environment variable HADOOP_HOME is missing")
  hadoopBin = file.path(hadoopHome, "bin")
  stream.jar = list.files(path=sprintf("%s/contrib/streaming", hadoopHome),pattern="jar$",full=TRUE)
  hadoop.command = sprintf("%s/hadoop jar %s ", hadoopBin,stream.jar)
  input =  make.input.files(in.folder)
  output = if(!missing(out.folder)) sprintf("-output %s",out.folder) else " "
  file = map.file
  inputformat = if(is.null(inputformat)){
    ' ' # default is TextInputFormat
  }else{
    sprintf(" -inputformat %s", inputformat)
  }
  outputformat = 'TextOutputFormat'
  mapper = sprintf('-mapper "Rscript %s -map" ',  tail(strsplit(map.file,"/")[[1]],1))
  if(!missing(reduce) ){
    if(is.character(reduce) && reduce=="aggregate"){
      reduce = sprintf('-reducer aggregate ')
      r.fl = " "
    } else{
      reduce = sprintf('-reducer "Rscript %s -reduce" ',  tail(strsplit(reduce.file,"/")[[1]],1))
      r.fl = sprintf("-file %s ",reduce.file)
    }
  }else {
    reduce=" ";r.fl = " "
  }
  m.fl = sprintf("-file %s ",map.file)
  
  if(!missing(numreduces)) numreduces = sprintf("-numReduceTasks %s ", numreduces) else numreduces = " "
  cmds = make.job.conf(otherparams, pfx="cmdenv")
  if(is.null(mapred$mapred.textoutputformat.separator)){
    if(!is.null(mpr.out)) mapred$mapred.textoutputformat.separator = sprintf("'%s'",mpr.out)
  }
  jobconfstring = make.job.conf(mapred,pfx="-D")

  caches = if(length(cachefiles)>0) make.cache.files(cachefiles,"-files") else " " #<0.21
  archives = if(length(archives)>0) make.cache.files(archives,"-archives") else " "
  mkjars = if(length(jarfiles)>0) make.cache.files(jarfiles,"-libjars",shorten=FALSE) else " "

  verb = if(verbose) "-verbose " else " "
  finalcommand = sprintf("%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s",
    hadoop.command,
    archives,
    caches,
    mkjars,
    jobconfstring,
    inputformat,
    input,
    output,
    mapper,
    reduce,
    m.fl,
    r.fl,
    image.cmd.line,
    cmds,
    numreduces,
    verb)
  if(debug)
    print(finalcommand)
  system(finalcommand)
}
     
rhtapply = function(X,
  INDEX,
  FUN,
  data,
  mpr.out="|",...,debug=FALSE){
  xtras =  list(...)
  output.sep = mpr.out
  colspec = list()
  for(i in INDEX){
    colspec[[as.character(i)]] = "character"
  }
  for(i in X){
    colspec[[as.character(i)]] = "numeric"
  }
  INDEX.l = 1:length(INDEX)
  X.D = (length(INDEX)+1):(length(INDEX)+length(X))
  anypremap = bquote({
    FUN = .(FUN);
    X.D = .(X.D)
    INDEX.l = .(INDEX.l)
  }, list(INDEX.l=INDEX.l, X.D=X.D,FUN=FUN))
  map = as.expression(bquote({
    if(length(INDEX.l)>1)
      m = split(d,as.list(d[,INDEX.l]),drop=TRUE)
    else
      m = split(d,d[,INDEX.l],drop=TRUE)
    m.names = names(m)
    for(x in m.names){
      FUN(k=x,d=m[[x]][,X.D,drop=FALSE])
    }
  }))
  map.f = function(d){}
  body(map.f) = map
  filename = tempfile("tapply")
  cmd = rhstream(
    map=map.f,
    reduce="aggregate",
    in.folder=data,
    out.folder=filename,
    mpr.out=output.sep,
    ANYPREMAP=anypremap,
    ...,
    debug=debug)
  if(!is.null(list(...)$verbose))
    cat(paste(outs,collapse="\n"))
  on.exit({
    if(!debug){
      hdfs.del(filename)
      unlink(filename)
    }
  })
  final = local({
    result = c()
    ff = hdfs.ls(filename)$file
    ff.w =  grep("part",ff)
    if(length(ff.w)==0) stop("No Output")
    ff = ff[ff.w]
    for(res in ff){
      f = hdfs.read.text.file(res)
      result = rbind(result,f)
    }
    broken = strsplit(result,output.sep,fixed=TRUE)
    counts = do.call("rbind",strsplit(unlist(lapply(broken,"[[",2)),"[[:space:]]"))
    levels.1 = do.call("rbind",strsplit(unlist(lapply(broken,"[[",1)),".",fixed=TRUE))
    data.frame(levels.1,count=counts,stringsAsFactors=FALSE)
  })
  final
}


## requires the presence of hdfs R package.
rhlapply = function(NorList,FUN,chunk.size=0,pre=NULL,debug=FALSE,.aggr=NULL,mapred=list(),keep=FALSE,mpr.out="!",ret.sep=",",...){

  pres = substitute(pre)
  RETSEP=ret.sep
  if(!is.null(pres)){
    if(class(pres)!="{") stop("'pre' should be a block e.g. { ... } ")
    anypremap = bquote({
      USERFUN = .(FUN)
      .(pres)
    },list(FUN=FUN, pres=pres))
  }else {
    anypremap = bquote({
      USERFUN = .(FUN)
      MYFSEP = .(FSEP)
    },list(FUN=FUN,FSEP=RETSEP))
  }
    
  temp.input = tempfile("rhlapply")
  temp.output = tempfile("rhlapply")
  on.exit({
    if(!debug){
      try({
        ## hdfs.del(temp.input)
        if(!keep) hdfs.del(temp.output)
        unlink(temp.output)
        unlink(temp.input)
      })}})
  colspec = NULL

  if(is.numeric(NorList)){
    archive = list.files(paste(system.file(package="RevoHStream"),"java",sep=.Platform$file.sep),pattern="jar$",full=T)
    mapred[['rhstream_numiterations']] = NorList
    if(max(chunk.size,0) >0 ) mapred$mapred.map.tasks = as.integer(chunk.size)
    .newmap = as.expression(bquote({
      lapply(d, function(r)
        send(c(Vec2Text(r[2]),Vec2Text(USERFUN(as.numeric(r[2])),collapse=MYFSEP)))
             )
    },list(FUN=FUN)))
    newmap = function(d) {}; body(newmap) = .newmap
    rhstream(map=newmap,
             in.folder=temp.input,
             out.folder=temp.output,
             inputformat="com.revolutionanalytics.hadoop.rhstr.LApplyInputFormat",
             mpr.out=mpr.out,numreduces=0,
             jarfiles=archive,debug=debug,mapred=mapred,ANYPREMAP=anypremap,...)
    final = local({
    result = c()
    index = c()
    ff = hdfs.ls(temp.output)$file
    ff.w =  grep("part",ff)
    if(length(ff.w)==0) stop("No Output")
    ff = ff[ff.w]
    for(res in ff){
      f = hdfs.read.text.file(res)
      if(is.null(f)) next
      broken = strsplit(f,mpr.out,fixed=TRUE)
      index = c(index,as.numeric(unlist(lapply(broken,"[[",1))))
      values = strsplit(unlist(lapply(broken,"[[",2)),ret.sep,fixed=TRUE)
      result = if(is.null(.aggr)){
        result = rbind(result,do.call("rbind", lapply(values,as.numeric)))
      }else{
        result = .aggr(values,result, index)
      }
    }
    if(is.null(.aggr)){
      if(ncol(result)!=1) result = t(result)
      result = cbind(index=index,result)
    }
    result
  })
  final
  }else{
    lk = as.list(NorList)
    archive = c()
    mapred = list()
  }
}

explode = function(path, key.sep,field.sep,cnames){

  textlines = data.frame(stringsAsFactors=FALSE)
  for(x in path)
    textlines = rbind(textlines, hdfs.read.text.file(x))
  s1 = do.call("rbind",strsplit(as.character(textlines[,1]),field.sep))
  if(!missing(key.sep))
    s2 = do.call("rbind",strsplit(s1[,1],key.sep))
  else
    s2 = s1[,1,drop=FALSE]
  ## length of colnames must be as long as number of columns of s2 + 1
  res = lapply(1:(length(cnames)-1),function(r){
    get(cnames[[ r ]],mode='function')( s2[,r] )
  })
  d = cbind(as.data.frame(res,stringsAsFactors=FALSE),  get(cnames[[ncol(s2)+1]],mode="function")(s1[,2]))
  colnames(d) = names(cnames)
  d
}


