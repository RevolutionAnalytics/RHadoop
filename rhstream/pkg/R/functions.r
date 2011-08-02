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
  if(missing(v)) {
    tmp = k
    k = tmp[[i]]
    v = tmp[-i]
    if (length(v) == 1) v = v[[1]]}
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

activateProfiling = function(){
  dir = file.path("/tmp/Rprof", Sys.getenv('mapred_job_id'), Sys.getenv('mapreduce_tip_id'))
  dir.create(dir, recursive = T)
  Rprof(file.path(dir, Sys.getenv('mapred_task_id')))}
  
deactivateProfiling = function() Rprof(NULL)

mapDriver = function(map, linebufsize, textinputformat, textoutputformat, profile){
  if(profile) activateProfiling()
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
  if(profile) deactivateProfiling()
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

flatten_list = function(l) if(is.list(l)) do.call(c, lapply(l, flatten_list)) else list(l)
to.data.frame = function(l) data.frame(do.call(rbind,lapply(l, function(r) as.data.frame(flatten_list(r)))))

dfs = function(cmd, ...) {
  if (is.null(names(list(...)))) {
    argnames = sapply(1:length(list(...)), function(i) "")
  }
  else {
    argnames = names(list(...))
  }
  system(paste(Sys.getenv("HADOOP_HOME"),
              "/bin/hadoop dfs -",
              cmd,
              " ",
              paste(
                apply(
                  cbind(
                    argnames, 
                    list(...)),
                  1, 
                  function(x) paste(
                    if(x[[1]] == ""){""} else{"-"},
                    x[[1]], 
                    " ", 
                    x[[2]], 
                    sep = ""))[
                      order(argnames, 
                            decreasing = T)], 
                collapse = " "),
               sep = ""),
         intern = T)
}

dfs.match = function(...) {
  cmd = strsplit(tail(as.character(as.list(match.call())[[1]]), 1), "\\.")[[1]][[2]]
  dfs(cmd, ...)
}

dfs.ls = dfs.match
dfs.lsr = dfs.match
dfs.df = dfs.match
dfs.du = dfs.match
dfs.dus = dfs.match
dfs.count = dfs.match
dfs.mv = dfs.match
dfs.cp = dfs.match
dfs.rm = dfs.match
dfs.rmr = dfs.match
dfs.expunge = dfs.match
dfs.put = dfs.match
dfs.copyFromLocal = dfs.match
dfs.moveFromLocal = dfs.match
dfs.get = dfs.match
dfs.getmerge = dfs.match
dfs.cat = dfs.match
dfs.text = dfs.match
dfs.copyToLocal = dfs.match
dfs.moveToLocal = dfs.match
dfs.mkdir = dfs.match
dfs.setrep = dfs.match
dfs.touchz = dfs.match
dfs.test = dfs.match
dfs.stat = dfs.match
dfs.tail = dfs.match
dfs.chmod = dfs.match
dfs.chown = dfs.match
dfs.chgrp = dfs.match
dfs.help = dfs.match

dfs.ls = dfs.match
dfs.get = dfs.match
dfs.put = dfs.match
dfs.rm = dfs.match
dfs.rmr = dfs.match
dfs.cat = dfs.match

dfs.exists = function(f) {
  length(dfs.ls(f)) == 0
}

toHDFSpath = function(input) {
  if (is.character(input)) {
    input}
  else {
    if(is.function(input)) {
      input()}}}

rhwrite = function(object, output = hdfs.tempfile(), textoutputformat = defaulttextoutputformat){
  if(is.data.frame(object)) {
    object = 
  }
  tmp = tempfile()
  hdfsOutput = toHDFSpath(output)
  cat(paste
       (lapply
        (object,
         function(x) {kv = keyval(x)
                      textoutputformat(kv$key, kv$val)}),
        collapse=""),
      file = tmp)
  dfs.put(tmp, hdfsOutput)
  file.remove(tmp)
  output
}

rhread = function(file, textinputformat = defaulttextinputformat, todataframe = F){
  tmp = tempfile()
  dfs.get(if(is.function(file)) {file()} else {file}, tmp)
  retval = if(file.info(tmp)[1,'isdir']) {
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
                  fname = eval(expression(fname), envir = e)
                  if(Sys.getenv("mapred_task_id") != "" && dfs.exists(fname)) dfs.rm(fname)
		})
  namefun
}

revoMapReduce = function(
  input,
  output = NULL,
  map,
  reduce = NULL,
  combine = NULL,
  verbose = FALSE,
  profilenodes = FALSE,
  inputformat = NULL,
  textinputformat = defaulttextinputformat,
  textoutputformat = defaulttextoutputformat) {

  on.exit(expr = gc()) #this is here to trigger cleanup of tempfiles
  if(!is.character(input) && !is.function(input))
    input = rhwrite(input)
  if (is.null(output)) output = hdfs.tempfile()
  
  rhstream(map = map,
           reduce = reduce,
           combine = combine,
           in.folder = toHDFSpath(input),
           out.folder = toHDFSpath(output),
           verbose = verbose,
           profilenodes = profilenodes,
           inputformat = inputformat,
           textinputformat = textinputformat,
           textoutputformat = textoutputformat)
  output
}

rhstream = function(
  map,
  reduce = NULL,
  combine = NULL,
  in.folder,
  out.folder, 
  linebufsize = 2000,
  verbose = FALSE,
  profilenodes = FALSE,
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
                                 else {RevoHStream:::defaulttextoutputformat},
              profile = profilenodes)'
  reduceLine  =  'RevoHStream:::reduceDriver(reduce = reduce,
                 linebufsize = linebufsize,
                 textinputformat = RevoHStream:::defaulttextinputformat,
                 textoutputformat = textoutputformat)'
  combineLine = 'RevoHStream:::reduceDriver(reduce = combine,
                 linebufsize = linebufsize,
                 textinputformat = RevoHStream:::defaulttextinputformat,
                 textoutputformat = RevoHStream:::defaulttextoutputformat)'

  map.file = tempfile(pattern = "rhstr.map")
  writeLines(c(lines,mapLine), con = map.file)
  reduce.file = tempfile(pattern = "rhstr.reduce")
  writeLines(c(lines, reduceLine), con = reduce.file)
  combine.file = tempfile(pattern = "rhstr.combine")
  writeLines(c(lines, combineLine), con = combine.file)
  ## set up the execution environment for map and reduce
  if (!is.null(combine) && is.logical(combine) && combine) {
    combine = reduce}
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
  m.fl = sprintf("-file %s ",map.file)
  if(!is.null(reduce) ){
    if(is.character(reduce) && reduce=="aggregate"){
      reducer = sprintf('-reducer aggregate ')
      r.fl = " "
    } else{
      reducer = sprintf('-reducer "Rscript %s" ',  tail(strsplit(reduce.file,"/")[[1]],1))
      r.fl = sprintf("-file %s ",reduce.file)
    }
    }else {
      reducer=" ";r.fl = " "
    }
  if(!is.null(combine) && is.function(combine)) {
    combiner = sprintf('-combiner "Rscript %s" ', tail(strsplit(combine.file, "/")[[1]],1))
    c.fl = sprintf("-file %s ", combine.file)}
  else {
    combiner = " "
    c.fl = " "}
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
    reducer,
    combiner,
    m.fl,
    r.fl,
    c.fl,
    image.cmd.line,
    cmds,
    numreduces,
    verb)
  if(debug)
    print(finalcommand)
  system(finalcommand)
}

