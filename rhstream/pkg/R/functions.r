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

mkMap = function(fun1 = identity, fun2 = identity) {
  if (missing(fun2)) {
    function(k,v) fun1(keyval(k,v))}
  else {
    function(k,v) keyval(fun1(k), fun2(v))}}

mkReduce = mkMap

mkLapplyReduce = function(fun1 = identity, fun2 = identity) {
  if (missing(fun2)) {
    function(k,vv) lapply(vv, function(v) fun1(keyval(k,v)))}
  else {
    function(k,vv) lapply(as.list(izip(fun1(k), fun2(v))), keyval)}}

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
## using isTRUE(all.equal(x)) because identical() was too strict, but on paper it should be it

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
                 out = NULL
                 out = reduce(g[[1]][[1]], getValues(g))
                 if(!is.null(out))
                   send(out, textoutputformat)
               })
      }
    }
    if (length(lastGroup) > 0) {
      out = reduce(lastKey, getValues(lastGroup))
      send(out, textoutputformat)
    }
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

rawtextinputformat = function(line) {keyval(NULL, line)}

to.data.frame = function(l) data.frame(do.call(rbind,lapply(l, function(x) if(is.atomic(x)) {x} else {do.call(function(...)c(..., recursive = T),x)})))

dfs = function(cmd, ...) {
  system(paste(Sys.getenv("HADOOP_HOME"),
               "/bin/hadoop dfs -",
               cmd,
               " ",
               paste(..., sep = " "),
               sep = ""),
         intern = T)
}

dfs.match = function(...) {
  cmd = strsplit(tail(as.character(as.list(match.call())[[1]]), 1), "\\.")[[1]][[2]]
  dfs(cmd, ...)
}

dfs.ls = dfs.match
dfs.get = dfs.match
dfs.put = dfs.match
dfs.rm = dfs.match
dfs.rmr = dfs.match

dfs.exists = function(f) {
  length(dfs.ls(f)) == 0
}

rhwrite = function(object, file = hdfs.tempfile(), textoutputformat = defaulttextoutputformat){
  tmp = tempfile()
  cat(paste
       (lapply
        (object,
         function(x) {kv = keyval(x)
                      textoutputformat(kv$key, kv$val)}),
        collapse=""),
      file = tmp)
  dfs.put(tmp, file)
  file.remove(tmp)
  file
}

rhread = function(file, textinputformat = defaulttextinputformat){
  tmp = tempfile()
  dfs.get(if(is.function(file)) {file()} else {file}, tmp)
  if(file.info(tmp)[1,'isdir']) {
    do.call(c,
            lapply(list.files(tmp, "part*"),
                   function(f) lapply(readLines(file.path(tmp, f)),
                                      textinputformat)))}      
    else {
      lapply(readLines(tmp), textinputformat)}
}

hdfs.tempfile <- function(pattern = "file", tmpdir = tempdir()) {
  fname  = tempfile(pattern, tmpdir)
  namefun = function() {fname}
  reg.finalizer(environment(namefun),
                function(e) {
                  print("finalizing")
                  fname = eval(expression(fname), envir = e)
                  if(Sys.getenv("mapred_task_id") != "" && dfs.exists(fname)) dfs.rm(fname)
		})
  namefun
}

revoMapReduce = function(
  input,
  output = hdfs.tempfile(),
  map,
  reduce = NULL,
  verbose = FALSE,
  inputformat = NULL,
  textinputformat = defaulttextinputformat,
  textoutputformat = defaulttextoutputformat) {

  on.exit(expr = gc())
  actualInput = NULL
  if (is.character(input)) {
    actualInput = input}
  else {
    if(is.function(input)) {
      actualInput = input()}
    else {
      actualInput = rhwrite(input)}
  }
        
  rhstream(map = map,
           reduce = reduce,
           in.folder = input,
           out.folder = if (is.function(output)) output() else output,
           verbose = verbose,
           inputformat = inputformat,
           textinputformat = textinputformat,
           textoutputformat = textoutputformat)
  output
}

rhstream = function(
  map,
  reduce = NULL,
  in.folder,
  out.folder, 
  linebufsize = 2000,
  verbose = FALSE,
  numreduces,
  cachefiles = c(),
  archives = c(),
  jarfiles = c(),
  otherparams = list(HADOOP_HOME = Sys.getenv('HADOOP_HOME'),
    HADOOP_CONF = Sys.getenv("HADOOP_CONF")),
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
              textoutputformat = if(is.null(reduce))
                                 {textoutputformat}
                                 else {RevoHStream:::defaulttextoutputformat})'
  reduceLine  =  'RevoHStream:::reduceDriver(reduce = reduce,
                 linebufsize = linebufsize,
                 textinputformat = RevoHStream:::defaulttextinputformat,
                 textoutputformat = textoutputformat)'
  
  map.file = tempfile(pattern = "rhstr.map")
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
  mapper = sprintf('-mapper "Rscript %s" ',  tail(strsplit(map.file,"/")[[1]],1))
  if(!is.null(reduce) ){
    if(is.character(reduce) && reduce=="aggregate"){
      reduce = sprintf('-reducer aggregate ')
      r.fl = " "
    } else{
      reduce = sprintf('-reducer "Rscript %s" ',  tail(strsplit(reduce.file,"/")[[1]],1))
      r.fl = sprintf("-file %s ",reduce.file)
    }
    }else {
      reduce=" ";r.fl = " "
    }
  m.fl = sprintf("-file %s ",map.file)
  
  if(!missing(numreduces)) numreduces = sprintf("-numReduceTasks %s ", numreduces) else numreduces = " "
  cmds = make.job.conf(otherparams, pfx="-cmdenv")
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

