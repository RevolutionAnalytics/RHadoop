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
keys = function(l) lapply(l, function(x) x[[1]])

values = function(l) lapply(l, function(x) x[[2]])

keyval = function(k, v = NULL, i = 1) {
  if(missing(v)) {
    tmp = k
    k = tmp[[i]]
    v = tmp[-i]
    if (length(v) == 1) v = v[[1]]}
  kv = list(key = k, val = v)
  attr(kv, 'keyval') = TRUE
  kv}

to.map = function(fun1 = identity, fun2 = identity) {
  if (missing(fun2)) {
    function(k,v) fun1(keyval(k,v))}
  else {
    function(k,v) keyval(fun1(k), fun2(v))}}

to.reduce = to.map

mkLapplyReduce = function(fun1 = identity, fun2 = identity) {
  if (missing(fun2)) {
    function(k,vv) lapply(vv, function(v) fun1(keyval(k,v)))}
  else {
    function(k,vv) lapply(vv, function(v) keyval(fun1(k), fun2(v)))}}

mkSeriesMap = function(map1, map2) function(k,v) {out = map1(k,v); map2(out$key, out$val)}
mkParallelMap = function(...) function (k,v) lapply(list(...), function(map) map(k,v))

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

reduceDriver = function(reduce, linebufsize, textinputformat, textoutputformat, reduceondataframe, profile){
    if(profile) activateProfiling()
    k = createReader(linebufsize, textinputformat)
    lastKey = NULL
    lastGroup = list()
    while( !is.null(d <- k$get())){
      d = c(lastGroup,d)
      lastKey =  d[[length(d)]][[1]]
      groupKeys = keys(d)
      lastGroup = d[listComp(groupKeys, lastKey)]
      d = d[!listComp(groupKeys, lastKey)]
      if(length(d) > 0) {
        groups = tapply(d, sapply(keys(d), digest), identity, simplify = FALSE)
        lapply(groups,
               function(g) {
                 out = NULL
                 out = reduce(g[[1]][[1]], if(reduceondataframe) {
                                             to.data.frame(values(g))}
                                          else {
                                            values(g)})
                 if(!is.null(out))
                   send(out, textoutputformat)
               })
      }
    }
    if (length(lastGroup) > 0) {
      out = reduce(lastKey, values(lastGroup))
      send(out, textoutputformat)
    }
    k$close()
    if(profile) deactivateProfiling()
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

encodeString = function(s) gsub("\\\n","\\\\n", gsub("\\\t","\\\\t", s))
decodeString = function(s) gsub("\\\\n","\\\n", gsub("\\\\t","\\\t", s))

defaulttextinputformat = function(line) {
  x =  strsplit(line, "\t")[[1]]
  keyval(fromJSON(decodeString(x[1])), fromJSON(decodeString(x[2])))}

defaulttextoutputformat = function(k,v) {
  paste(encodeString(toJSON(k, collapse = "")), "\t", encodeString(toJSON(v, collapse = "")), "\n", sep = "")}
rawtextinputformat = function(line) {keyval(NULL, line)}

flatten = function(x) unlist(list(name = as.name("name"), x))[-1]
to.data.frame = function(l) data.frame(lapply(data.frame(do.call(rbind,lapply(l, flatten))), unlist))
from.data.frame = function(df, keycol = 1) lapply(1:dim(df)[[1]], function(i) keyval(df[i,], i = keycol))

dfs = function(cmd, intern, ...) {
  if (is.null(names(list(...)))) {
    argnames = sapply(1:length(list(...)), function(i) "")
  }
  else {
    argnames = names(list(...))
  }
  system(paste(Sys.getenv("HADOOP_HOME"), "/bin/hadoop dfs -", cmd, " ",
              paste(
                apply(cbind(argnames, list(...)),1, 
                  function(x) paste(
                    if(x[[1]] == ""){""} else{"-"},
                    x[[1]], 
                    " ", 
                    to.hdfs.path(x[[2]]), 
                    sep = ""))[
                      order(argnames, decreasing = T)], 
                collapse = " "),
               sep = ""),
         intern = intern)}

getcmd = function(matched.call)
  strsplit(tail(as.character(as.list(matched.call)[[1]]), 1), "\\.")[[1]][[2]]
           
dfs.match.sideeffect = function(...) {
  dfs(getcmd(match.call()), FALSE, ...) == 0}

dfs.match.out = function(...)
  to.data.frame(strsplit(dfs(getcmd(match.call()), TRUE, ...)[-1], " +"))

mkdfsfun = function(dfscmd, out)
  eval(parse(text = paste ("dfs.", dfscmd, " = dfs.match.", if(out) "out" else "sideeffect", sep = "")),
       envir = parent.env(environment()))

for (dfscmd in c("ls","lsr","df","du","dus","count","cat","text","stat","tail","help")) 
  mkdfsfun(dfscmd, TRUE)

for (dfscmd in c("mv","cp","rm","rmr","expunge","put","copyFromLocal","moveFromLocal","get","getmerge",
                 "copyToLocal","moveToLocal","mkdir","setrep","touchz","test","chmod","chown","chgrp"))
  mkdfsfun(dfscmd, FALSE)

dfs.exists = function(f) dfs.test(e = f) 
dfs.empty = function(f) dfs.test(z = f) 
dfs.is.dir = function(f) dfs.test(d = f)

to.hdfs.path = function(input) {
  if (is.character(input)) {
    input}
  else {
    if(is.function(input)) {
      input()}}}

to.dfs = function(object, file = hdfs.tempfile(), textoutputformat = defaulttextoutputformat){
  if(is.data.frame(object)) {
    object = from.data.frame(object)
  }
  tmp = tempfile()
  hdfsOutput = to.hdfs.path(file)
  cat(paste
       (lapply
        (object,
         function(x) {kv = keyval(x)
                      textoutputformat(kv$key, kv$val)}),
        collapse=""),
      file = tmp)
  dfs.put(tmp, hdfsOutput)
  file.remove(tmp)
  file
}

from.dfs = function(file, textinputformat = defaulttextinputformat, todataframe = F){
  tmp = tempfile()
  dfs.get(to.hdfs.path(file), tmp)
  retval = if(file.info(tmp)[1,'isdir']) {
             do.call(c,
               lapply(list.files(tmp, "part*"),
                 function(f) lapply(readLines(file.path(tmp, f)),
                            textinputformat)))}      
          else {
            lapply(readLines(tmp), textinputformat)}
  if(!todataframe) {
    retval}
  else{
    to.data.frame(retval)  }
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

mapreduce = function(
  input,
  output = NULL,
  map = to.map(identity),
  reduce = NULL,
  combine = NULL,
  reduceondataframe = FALSE,
  profilenodes = FALSE,
  inputformat = NULL,
  outputformat = NULL,
  textinputformat = defaulttextinputformat,
  textoutputformat = defaulttextoutputformat,
  verbose = FALSE) {

  on.exit(expr = gc()) #this is here to trigger cleanup of tempfiles
  if (is.null(output)) output = hdfs.tempfile()
  
  rhstream(map = map,
           reduce = reduce,
           reduceondataframe = reduceondataframe,
           combine = combine,
           in.folder = if(is.list(input)) {lapply(input, to.hdfs.path)} else to.hdfs.path(input),
           out.folder = to.hdfs.path(output),
           profilenodes = profilenodes,
           inputformat = inputformat,
           outputformat = outputformat,
           textinputformat = textinputformat,
           textoutputformat = textoutputformat,
           verbose = verbose)
  output
}

rhstream = function(
  map,
  reduce = NULL,
  reduceondataframe = F,
  combine = NULL,
  in.folder,
  out.folder, 
  linebufsize = 2000,
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
  outputformat = NULL,
  textinputformat = defaulttextinputformat,
  textoutputformat = defaulttextoutputformat,
  verbose = FALSE,
  debug = FALSE) {
    ## prepare map and reduce executables
  lines = '#! /usr/bin/env Rscript
options(warn=-1)

library(rmr)
load("rmrParentEnv")
load("rmrLocalEnv")
'

  mapLine = 'rmr:::mapDriver(map = map,
              linebufsize = linebufsize,
              textinputformat = textinputformat,
              textoutputformat = if(is.null(reduce))
                                 {textoutputformat}
                                 else {rmr:::defaulttextoutputformat},
              profile = profilenodes)'
  reduceLine  =  'rmr:::reduceDriver(reduce = reduce,
                 linebufsize = linebufsize,
                 textinputformat = rmr:::defaulttextinputformat,
                 textoutputformat = textoutputformat,
                 reduceondataframe = reduceondataframe,
                 profile = profilenodes)'
  combineLine = 'rmr:::reduceDriver(reduce = combine,
                 linebufsize = linebufsize,
                 textinputformat = rmr:::defaulttextinputformat,
                 textoutputformat = rmr:::defaulttextoutputformat,
                 reduceondataframe = reduceondataframe,
                profile = profilenodes)'

  map.file = tempfile(pattern = "rhstr.map")
  writeLines(c(lines,mapLine), con = map.file)
  reduce.file = tempfile(pattern = "rhstr.reduce")
  writeLines(c(lines, reduceLine), con = reduce.file)
  combine.file = tempfile(pattern = "rhstr.combine")
  writeLines(c(lines, combineLine), con = combine.file)
  ## set up the execution environment for map and reduce
  if (!is.null(combine) && is.logical(combine) && combine) {
    combine = reduce}
  rmrParentEnv = file.path(tempdir(), "rmrParentEnv")
  rmrLocalEnv = file.path(tempdir(), "rmrLocalEnv")
  save.image(file = rmrParentEnv)
  save(list = ls(all = TRUE, envir = environment()), file = rmrLocalEnv, envir = environment())
  image.cmd.line = paste("-file", rmrParentEnv, "-file", rmrLocalEnv)
  
  ## prepare hadoop streaming command
  hadoopHome = Sys.getenv("HADOOP_HOME")
  if(hadoopHome == "") warning("Environment variable HADOOP_HOME is missing")
  hadoopBin = file.path(hadoopHome, "bin")
  stream.jar = list.files(path=sprintf("%s/contrib/streaming", hadoopHome),pattern="jar$",full=TRUE)
  hadoop.command = sprintf("%s/hadoop jar %s ", hadoopBin,stream.jar)
  input =  make.input.files(in.folder)
  output = if(!missing(out.folder)) sprintf("-output %s",out.folder) else " "
  inputformat = if(is.null(inputformat)){
    ' ' # default is TextInputFormat
  }else{
    sprintf(" -inputformat %s", inputformat)
  }
  outputformat = if(is.null(outputformat)){
    ' '}
  else {
    sprintf(" -outputformat %s", outputformat)
  }
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
  retval = system(finalcommand)
if (retval != 0) stop("hadoop streaming failed with error code ", retval, "\n")
}

