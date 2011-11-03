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

#options

rmr.options = new.env(parent=emptyenv())
rmr.options$backend = "hadoop"
rmr.options$profilenodes = FALSE

rmr.profilenodes = function(on = NULL) {
  if (!is.null(on))
    rmr.options$profilenodes = on
  rmr.options$profilenodes}

rmr.backend = function(backend = c(NULL, "hadoop", "local")) {
  if(!missing(backend)){
    backend = match.arg(backend)
    rmr.options$backend = backend}
  rmr.options$backend}

#I/O
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
  if (is.keyval(out)) 
    cat(textoutputformat(out$key, out$val))
  else
    lapply(out, function(o) cat(textoutputformat(o$key, o$val)) )
  TRUE 
}

# additional hadoop features, disabled for now
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

keyval = function(k, v) {
  kv = list(key = k, val = v)
  attr(kv, 'keyval') = TRUE
  kv}

is.keyval = function(kv) !is.null(attr(kv, 'keyval', exact = TRUE))

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

## drivers section, or what runs on the nodes

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
      out = reduce(lastKey, 
                   if(reduceondataframe) {
                     to.data.frame(values(lastGroup))}
                   else {
                     values(lastGroup)})
      send(out, textoutputformat)
    }
    k$close()
    if(profile) deactivateProfiling()
    invisible()
}

#some option formatting utils

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

# formats
# alternate, native format
# unserialize(charToRaw(gsub("\\\\n", "\n", gsub("\n", "\\\\n", rawToChar(serialize(matrix(1:20, ncol = 5), ascii=T,conn = NULL))))))

nativetextinputformat = function(line) {
  x = strsplit(line, "\t")[[1]]
  de = function(x) unserialize(charToRaw(gsub("\\\\n", "\n", x)))
  keyval(de(x[1]),de(x[2]))}

nativetextoutputformat = function(k, v) {
  ser = function(x) gsub("\n", "\\\\n", rawToChar(serialize(x, ascii=T,conn = NULL)))
  paste(ser(k), "\t", ser(v), "\n", sep = "")}

  
defaulttextinputformat = function(line) {
  decodeString = function(s) gsub("\\\\n","\\\n", gsub("\\\\t","\\\t", s))
  x =  strsplit(line, "\t")[[1]]
  keyval(fromJSON(decodeString(x[1]), asText = TRUE), 
         fromJSON(decodeString(x[2]), asText = TRUE))}

defaulttextoutputformat = function(k,v) {
  encodeString = function(s) gsub("\\\n","\\\\n", gsub("\\\t","\\\\t", s))
  paste(encodeString(toJSON(k, collapse = "")), "\t", encodeString(toJSON(v, collapse = "")), "\n", sep = "")}

rawtextinputformat = function(line) {keyval(NULL, line)}
csvtextinputformat = function(key = 1, ...) function(line) {tc = textConnection(line)
                                                            df = read.table(file = tc, header = FALSE, ...)
                                                            close(tc)
                                                            keyval(df[,key], df[,-key])}

rawtextoutputformat = function(k,v) paste(c(k,v, "\n"), collapse = "")
csvtextoutputformat = function(...) function(k,v) {
  tc = textConnection(object = NULL, open = "w")
  args = list(x = c(as.list(k),as.list(v)), file = tc, ..., row.names = FALSE, col.names = FALSE)
  do.call(write.table, args[unique(names(args))])
  paste(textConnectionValue(con = tc), "\n", sep = "", collapse = "")}

#data frame conversion

flatten = function(x) unlist(list(name = as.name("name"), x))[-1]
to.data.frame = function(l) data.frame(lapply(data.frame(do.call(rbind,lapply(l, flatten))), unlist))
from.data.frame = function(df, keycol = 1) lapply(1:dim(df)[[1]], function(i) keyval(df[i,keycol],df[i,] ))

#output cmp
cmp = function(x, y) isTRUE(all.equal(x[order(unlist(keys(x)))], 
                                      y[order(unlist(keys(y)))]))

#hdfs section

hdfs = function(cmd, intern, ...) {
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
                    to.dfs.path(x[[2]]), 
                    sep = ""))[
                      order(argnames, decreasing = T)], 
                collapse = " "),
               sep = ""),
         intern = intern)}

getcmd = function(matched.call)
  strsplit(tail(as.character(as.list(matched.call)[[1]]), 1), "\\.")[[1]][[2]]
           
hdfs.match.sideeffect = function(...) {
  hdfs(getcmd(match.call()), FALSE, ...) == 0}

hdfs.match.out = function(...)
  to.data.frame(strsplit(hdfs(getcmd(match.call()), TRUE, ...)[-1], " +"))

mkhdfsfun = function(hdfscmd, out)
  eval(parse(text = paste ("hdfs.", hdfscmd, " = hdfs.match.", if(out) "out" else "sideeffect", sep = "")),
       envir = parent.env(environment()))

for (hdfscmd in c("ls","lsr","df","du","dus","count","cat","text","stat","tail","help")) 
  mkhdfsfun(hdfscmd, TRUE)

for (hdfscmd in c("mv","cp","rm","rmr","expunge","put","copyFromLocal","moveFromLocal","get","getmerge",
                 "copyToLocal","moveToLocal","mkdir","setrep","touchz","test","chmod","chown","chgrp"))
  mkhdfsfun(hdfscmd, FALSE)

# backend independent dfs section
dfs.exists = function(f) {
  if (rmr.backend() == 'hadoop') 
    hdfs.test(e = f) 
  else file.exists(f)}

dfs.rm = function(f){
  if(rmr.backend() == 'hadoop')
    hdfs.rm(f)
  else file.remove(f)}

dfs.is.dir = function(f) { 
  if (rmr.backend() == 'hadoop') 
    hdfs.test(d = f)
  else file.info(f)['isdir']}

dfs.empty = function(f) {
  if(rmr.backend() == 'hadoop') {
    if(dfs.is.dir(f)) {
      hdfs.test(z = file.path(to.dfs.path(f), 'part-00000'))}
    else {hdfs.test(z = f)}}
  else file.info(f)['size'] == 0}

# dfs bridge

to.dfs.path = function(input) {
  if (is.character(input)) {
    input}
  else {
    if(is.function(input)) {
      input()}}}

to.dfs = function(object, file = dfs.tempfile(), textoutputformat = defaulttextoutputformat){
  if(is.data.frame(object)) {
    object = from.data.frame(object)
  }
  tmp = tempfile()
  dfsOutput = to.dfs.path(file)
  cat(paste
       (lapply
        (object,
         function(x) {kv = if (is.keyval(x)) x else keyval(NULL, x)
                      textoutputformat(kv$key, kv$val)}),
        collapse=""),
      file = tmp)
  if(rmr.backend() == 'hadoop'){
    hdfs.put(tmp, dfsOutput)
    file.remove(tmp)}
  else
    file.rename(tmp, dfsOutput)
  file
}

from.dfs = function(file, textinputformat = defaulttextinputformat, todataframe = F){
  if(rmr.backend() == 'hadoop') {
    tmp = tempfile()
    hdfs.get(to.dfs.path(file), tmp)}
  else tmp = to.dfs.path(file)
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

# mapreduce

dfs.tempfile <- function(pattern = "file", tmpdir = tempdir()) {
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
  inputformat = NULL,
  outputformat = NULL,
  textinputformat = defaulttextinputformat,
  textoutputformat = defaulttextoutputformat,
  verbose = FALSE) {

  on.exit(expr = gc(), add = TRUE) #this is here to trigger cleanup of tempfiles
  if (is.null(output)) output = dfs.tempfile()

  backend  =  rmr.options$backend
  
  profilenodes = rmr.options$profilenodes
    
  mr = switch(backend, hadoop = rhstream, local = mr.local, stop("Unsupported backend: ", backend))
  
  mr(map = map,
     reduce = reduce,
     reduceondataframe = reduceondataframe,
     combine = combine,
     in.folder = if(is.list(input)) {lapply(input, to.dfs.path)} else to.dfs.path(input),
     out.folder = to.dfs.path(output),
     profilenodes = profilenodes,
     inputformat = inputformat,
     outputformat = outputformat,
     textinputformat = textinputformat,
     textoutputformat = textoutputformat,
     verbose = verbose)
  output
}

# backends

mr.local = function(map,
                    reduce,
                    reduceondataframe = FALSE,
                    combine = NULL,
                    in.folder,
                    out.folder,
                    profilenodes = FALSE,
                    inputformat = NULL,
                    outputformat = NULL,
                    textinputformat = defaulttextinputformat,
                    textoutputformat = defaulttextoutputformat,
                    verbose = verbose) {
  if(is.null(reduce)) reduce = function(k,vv) lapply(vv, function(v) keyval(k,v))
  map.out = do.call(c, 
                    lapply(do.call(c,
                                   lapply(in.folder, 
                                          function(x) lapply(from.dfs(x, 
                                                                      textinputformat = textinputformat),
                                                             function(y){attr(y$key, "input") = x; y}))), 
                              function(kv) {retval = map(kv$key, kv$val)
                                            if(is.keyval(retval)) list(retval)
                                            else retval}))
  map.out = from.dfs(to.dfs(map.out))
  reduce.out = tapply(X = map.out, 
                      INDEX = sapply(keys(map.out), digest), 
                      FUN = function(x) reduce(x[[1]]$key, 
                                             if(reduceondataframe) to.data.frame(values(x)) else values(x)),
                      simplify = FALSE)
  if(!is.keyval(reduce.out[[1]]))
    reduce.out = do.call(c, reduce.out)
  names(reduce.out) = replicate(n=length(names(reduce.out)), "")
  to.dfs(reduce.out, out.folder)}

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
options(warn=1)

library(rmr)
load("rmr-local-env")
  
'  

  mapLine = 'load("rmr-map-env", envir = environment(map))
  rmr:::mapDriver(map = map,
              linebufsize = linebufsize,
              textinputformat = textinputformat,
              textoutputformat = if(is.null(reduce))
                                 {textoutputformat}
                                 else {rmr:::defaulttextoutputformat},
              profile = profilenodes)'
  reduceLine  =  'load("rmr-reduce-env", envir = environment(reduce))
  rmr:::reduceDriver(reduce = reduce,
                 linebufsize = linebufsize,
                 textinputformat = rmr:::defaulttextinputformat,
                 textoutputformat = textoutputformat,
                 reduceondataframe = reduceondataframe,
                 profile = profilenodes)'
  combineLine = 'load("rmr-combine-env", envir = environment(combine))
 rmr:::reduceDriver(reduce = combine,
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

  save.env = function(fun = NULL, name) {
    fun.env = file.path(tempdir(), name)
    envir = if(is.null(fun)) parent.env(environment()) else environment(fun)
    save(list = ls(all = TRUE, envir = envir), file = fun.env, envir = envir)
    fun.env}

  image.cmd.line = paste("-file",
                         c(save.env(name = "rmr-local-env"),
                          save.env(map, "rmr-map-env"),
                          if(is.function(reduce)) {
                            save.env(reduce, "rmr-reduce-env")},
                          if(is.function(combine))   
                            save.env(combine, "rmr-combine-env")), 
                        collapse=" ")
  
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
  
  #debug.opts = "-mapdebug kdfkdfld -reducexdebug jfkdlfkja"
  caches = if(length(cachefiles)>0) make.cache.files(cachefiles,"-files") else " " #<0.21
  archives = if(length(archives)>0) make.cache.files(archives,"-archives") else " "
  mkjars = if(length(jarfiles)>0) make.cache.files(jarfiles,"-libjars",shorten=FALSE) else " "
  
  verb = if(verbose) "-verbose " else " "
  finalcommand = 
    paste(
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
   #   debug.opts,
      verb)
  retval = system(finalcommand)
if (retval != 0) stop("hadoop streaming failed with error code ", retval, "\n")
}


 
## a sort of relational join very useful in a variety of map reduce algorithms

## to.dfs(lapply(1:10, function(i) keyval(i, i^2)), "/tmp/reljoin.left")
## to.dfs(lapply(1:10, function(i) keyval(i, i^3)), "/tmp/reljoin.right")
## equijoin(leftinput="/tmp/reljoin.left", rightinput="/tmp/reljoin.right", output = "/tmp/reljoin.out")
## from.dfs("/tmp/reljoin.out")

equijoin = function(
  leftinput = NULL,
  rightinput = NULL,
  input = NULL,
  output = NULL,
  outer = c("", "left", "right", "full"),
  map.left = to.map(identity),
  map.right = to.map(identity),
  reduce  = function(k, values.left, values.right)
    do.call(c,
            lapply(values.left,
                   function(vl) lapply(values.right,
                                       function(vr) reduceall(k, vl, vr)))),
  reduceall  = function(k,vl,vr) keyval(k, list(left = vl, right = vr)))
{
  stopifnot(xor(!is.null(leftinput), !is.null(input) &&
                (is.null(leftinput)==is.null(rightinput))))
  outer = match.arg(outer)
  leftouter = outer == "left"
  rightouter = outer == "right"
  fullouter = outer == "full"
  if (is.null(leftinput)) {
    leftinput = input}
  markSide =
    function(kv, isleft) keyval(kv$key, list(val = kv$val, isleft = isleft))
  isLeftSide = 
    function(leftinput) {
      leftin = strsplit(to.dfs.path(leftinput), "/+")[[1]]
      mapin = strsplit(Sys.getenv("map_input_file"), "/+")[[1]]
      leftin = leftin[-1]
      mapin = mapin[if(mapin[1] == "hdfs:") c(-1,-2) else -1]
      all(mapin[1:length(leftin)] == leftin)}
  reduce.split =
    function(vv) tapply(lapply(vv, function(v) v$val), sapply(vv, function(v) v$isleft), identity, simplify = FALSE)
  padSide =
    function(vv, sideouter, fullouter) if (length(vv) == 0 && (sideouter || fullouter)) c(NA) else vv
  map = if (is.null(input)) {
    function(k,v) {
      ils = switch(rmr.backend(), 
                   hadoop = isLeftSide(leftinput),
                   local = attr(k, "input") == to.dfs.path(leftinput),
                   stop("Unsupported backend: ", rmr.backend()))
      markSide(if(ils) map.left(k,v) else map.right(k,v), ils)}}
  else {
    function(k,v) {
      list(markSide(map.left(k,v), TRUE),
           markSide(map.right(k,v), FALSE))}}
  eqj.reduce = reduce
  mapreduce(map = map,
            reduce =
            function(k, vv) {
              rs = reduce.split(vv)
              eqj.reduce(k,
                     padSide(rs$`TRUE`, rightouter, fullouter),
                     padSide(rs$`FALSE`, leftouter, fullouter))},
            input = c(leftinput,rightinput),
            output = output)}

