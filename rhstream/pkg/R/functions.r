make.job.conf = function(m,pfx){
  N = names(m)
  if(length(m) == 0) return(" ")
  paste(unlist(lapply(1:length(N),function(i){
    sprintf("%s %s=%s ", pfx, N[[i]],as.character(m[[i]]))
  })),
        collapse = " ")
}

make.cache.files = function(caches,pfx,shorten = TRUE){
  if(length(caches) == 0) return(" ")
  sprintf("%s %s",pfx, paste(sapply(caches,function(r){
    if(shorten) {
      path.leaf = tail(strsplit(r,"/")[[1]],1)
      sprintf("'%s#%s'",r,path.leaf)
    }else{
      sprintf("'%s'",r)
    }
  }),
                             collapse = ","))
}

make.input.files = function(infiles){
  if(length(infiles) == 0) return(" ")
  paste(sapply(infiles, function(r){
    sprintf("-input %s ", r)
  }),
        collapse=" ")
}

defaulttextinputformat = function(lines) {
  lapply(strsplit(lines, "\t"),
         function(x) lapply(x, fromJSON)) }

defaulttextoutputformat = function(k,v) {
  paste(toJSON(k), "\t", toJSON(v), "\n", sep = "")}

## The function to run a map and reduce over a hadoop cluster does not implement
## a general combine unless it is one of the streaminga default combine.

rhstream = function(
  map,
  reduce,
  in.folder,
  out.folder, 
  N = 2000,
  verbose = FALSE,
  numreduces,
  cachefiles = c(),
  archives = c(),
  jarfiles = c(),
  otherparams = list(),
  mapred = list(),
  mpr.out = NULL,
  ANYPREMAP = {},
  ANYPREREDUCE = {},
  inputformat = NULL,
  textinputformat = defaulttextinputformat,
  textoutputformat = defaulttextoutputformat,
  debug = FALSE)
  
{
  on.exit({
    if(!debug)
      unlink(map.file)
  })
  template.file = paste(
    system.file(package="RevoHStream"),
    "ec2mr.r",
    sep=.Platform$file.sep)
  lines = readLines(template.file)
  map.driver = deparse(bquote({
    MAP = .(MAP)
    N = .(N)
    TEXTINPUTFORMAT = .(TEXTINPUTFORMAT)
    TEXTOUTPUTFORMAT = .(TEXTOUTPUTFORMAT)
    .(ANYPREMAP)
    driverFunction(MAP, N, TEXTINPUTFORMAT, TEXTOUTPUTFORMAT)
  },
    list(MAP = map,
         N = N,
         ANYPREMAP = ANYPREMAP,
         TEXTINPUTFORMAT = textinputformat,
         TEXTOUTPUTFORMAT = if(missing(reduce)) {textoutputformat} else {defaulttextoutputformat})))
  if(!missing(reduce)){
    if(is.character(reduce) && reduce == "aggregate"){
    }else{
      reduce.driver = deparse(bquote({
        REDUCE = .(REDUCE)
        N = .(N)
        TEXTINPUTFORMAT = .(TEXTINPUTFORMAT)
        TEXTOUTPUTFORMAT = .(TEXTOUTPUTFORMAT)
        .(ANYPREREDUCE)
        driverFunction(REDUCE, N, TEXTINPUTFORMAT, TEXTOUTPUTFORMAT)},
        list(REDUCE = reduce,
             N = N,
             ANYPREREDUCE = ANYPREREDUCE,
             TEXTINPUTFORMAT = defaulttextinputformat,
             TEXTOUTPUTFORMAT = textoutputformat)))
      reduce.file = tempfile(pattern = "rhstr.reduce")
      writeLines(c(lines, reduce.driver), con = reduce.file)
      on.exit({
        if(!debug) unlink(reduce.file)
      },
              add = TRUE)
    }
  }
  map.file = tempfile(pattern="rhstr.map")
  if(debug)
    message(sprintf("Temporary file:%s", map.file))
  writeLines(c(lines,map.driver), con = map.file)
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
  finalcommand = sprintf("%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s",
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
  ##("/airline/tmp/subset",key.sep="∫",field.sep="\t",colnames=c("dow"="as.character","hod"="as.character","count"="as.numeric"))
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


## explode("/tmp/RtmpkejWHO/hlapply643c9869",field.sep="∫",cnames=list('x'='as.numeric','y'='as.numeric'))
