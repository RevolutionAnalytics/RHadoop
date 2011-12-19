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
rmr.options$profile.nodes = FALSE
rmr.options$depend.check = FALSE
rmr.options$managed.dir = "/var/rmr/managed"

rmr.options.get = function(...){
  opts = as.list(rmr.options)
  if(missing(...))
    opts
  else {
    args = c(...)
    if (length(args) > 1)
      opts[args]
    else 
      opts[[args]]}}

rmr.options.set = function(backend = c("hadoop", "local"),
                           profile.nodes = NULL,
                           depend.check = NULL,
                           managed.dir = NULL) {
  this.call = match.call()
  backend = match.arg(backend)
  lapply(names(this.call)[-1], 
         function(x) 
           assign(x, eval(this.call[[x]]), envir = rmr.options))
  as.list(rmr.options)}

#I/O
createReader = function(line.buffer.size = 2000, text.input.format){
  con = file("stdin", open="r")
  close = function(){
    ## close(con)
  }
  readChunk = function(){
    lines = readLines(con = con, n = line.buffer.size, warn = FALSE)
    if(length(lines) > 0){
      ll = lapply(lines, text.input.format)
      return(ll[!sapply(ll, is.null)])
    }else{
      return(NULL) 
    }
  }
  return(list(close = close, get = readChunk))
}

send = function(out, text.output.format = default.text.output.format){
  if (is.keyval(out)) 
    cat(text.output.format(out$key, out$val))
  else
    lapply(out, function(o) cat(text.output.format(o$key, o$val)) )
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
## keyval manip
keyval = function(k, v) {
  kv = list(key = k, val = v)
  attr(kv, 'rmr.keyval') = TRUE
  kv}

is.keyval = function(kv) !is.null(attr(kv, 'rmr.keyval', exact = TRUE))
keys = function(l) lapply(l, function(x) x[[1]])
values = function(l) lapply(l, function(x) x[[2]])
keyval.to.list = function(kvl) {l = values(kvl); names(l) = keys(kvl); l}

## map and reduce function generation

to.map = function(fun1, fun2 = identity) {
  if (missing(fun2)) {
    function(k,v) fun1(keyval(k,v))}
  else {
    function(k,v) keyval(fun1(k), fun2(v))}}

to.reduce = to.map

to.reduce.all = function(fun1, fun2 = identity) {
  if (missing(fun2)) {
    function(k,vv) lapply(vv, function(v) fun1(keyval(k,v)))}
  else {
    function(k,vv) lapply(vv, function(v) keyval(fun1(k), fun2(v)))}}

## mapred combinators
wrap.keyval = function(kv) {
  if(is.null(kv)) list()
  else if (is.keyval(kv)) list(kv) 
  else kv}

compose.mapred = function(mapred, map) function(k,v) {
  out = mapred(k,v)
  if (is.null(out)) NULL
  else if (is.keyval(out)) map(out$key, out$val)
  else  do.call(c,
                lapply(out, 
                       function(x) 
                         wrap.keyval(map(x$key, x$val))))}

union.mapred = function(mr1, mr2) function(k,v) {
  out = c(wrap.keyval(mr1(k,v)), wrap.keyval(mr2(k,v)))
  if (length(out) == 0) NULL else out}


## drivers section, or what runs on the nodes

activate.profiling = function(){
  dir = file.path("/tmp/Rprof", Sys.getenv('mapred_job_id'), Sys.getenv('mapred_tip_id'))
  dir.create(dir, recursive = T)
  Rprof(file.path(dir, paste(Sys.getenv('mapred_task_id'), Sys.time())), interval=0.000000001)}
  
close.profiling = function() Rprof(NULL)

map.driver = function(map, line.buffer.size, text.input.format, text.output.format, profile){
  if(profile) activate.profiling()
  k = createReader(line.buffer.size, text.input.format)
  while( !is.null(d <- k$get())){
      lapply(d,
             function(r) {
                 out = map(r[[1]], r[[2]])
                 if(!is.null(out))
                     send(out, text.output.format)
             })
  }
  k$close()
  if(profile) close.profiling()
  invisible()
}

listComp = function(ll,e) sapply(ll, function(l) isTRUE(all.equal(e,l)))
## using isTRUE(all.equal(x)) because identical() was too strict, but on paper it should be it

reduce.driver = function(reduce, line.buffer.size, text.input.format, text.output.format, reduce.on.data.frame, profile){
    if(profile) activate.profiling()
    k = createReader(line.buffer.size, text.input.format)
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
                 out = reduce(g[[1]][[1]], if(reduce.on.data.frame) {
                                             list.to.data.frame(values(g))}
                                          else {
                                            values(g)})
                 if(!is.null(out))
                   send(out, text.output.format)
               })
      }
    }
    if (length(lastGroup) > 0) {
      out = reduce(lastKey, 
                   if(reduce.on.data.frame) {
                     list.to.data.frame(values(lastGroup))}
                   else {
                     values(lastGroup)})
      send(out, text.output.format)
    }
    k$close()
    if(profile) close.profiling()
    invisible()
}

#some option formatting utils

paste.options = function(optlist) {
  optlist = unlist(sapply(optlist, function(x) if (is.logical(x)) {if(x) "" else NULL} else x))
  paste(unlist(rbind(paste("-", names(optlist), sep = ""), optlist)), collapse = " ")
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

# formats
# alternate, native format
# unserialize(charToRaw(gsub("\\\\n", "\n", gsub("\n", "\\\\n", rawToChar(serialize(matrix(1:20, ncol = 5), ascii=T,conn = NULL))))))

native.text.input.format = function(line) {
  x = strsplit(line, "\t")[[1]]
  de = function(x) unserialize(charToRaw(gsub("\\\\n", "\n", x)))
  keyval(de(x[1]),de(x[2]))}

native.text.output.format = function(k, v) {
  ser = function(x) gsub("\n", "\\\\n", rawToChar(serialize(x, ascii=T,conn = NULL)))
  paste(ser(k), "\t", ser(v), "\n", sep = "")}

  
json.text.input.format = function(line) {
  decodeString = function(s) gsub("\\\\n","\\\n", gsub("\\\\t","\\\t", s))
  x =  strsplit(line, "\t")[[1]]
  keyval(fromJSON(decodeString(x[1]), asText = TRUE), 
         fromJSON(decodeString(x[2]), asText = TRUE))}

json.text.output.format = function(k,v) {
  encodeString = function(s) gsub("\\\n","\\\\n", gsub("\\\t","\\\\t", s))
  paste(encodeString(toJSON(k, collapse = "")), "\t", encodeString(toJSON(v, collapse = "")), "\n", sep = "")}

raw.text.input.format = function(line) {keyval(NULL, line)}
csv.text.input.format = function(key = 1, ...) function(line) {
  tc = textConnection(line)
  df = tryCatch(read.table(file = tc, header = FALSE, ...),
                error = 
                  function(e) {
                    if (e$message == "no lines available in input") 
                      write(x="No data in this line", file=stderr()) 
                    else stop(e)
                    NULL})
  close(tc)
  keyval(df[,key], df[,-key])}

raw.text.output.format = function(k,v) paste(c(k,v, "\n"), collapse = "")
csv.text.output.format = function(...) function(k,v) {
  tc = textConnection(object = NULL, open = "w")
  args = list(x = c(as.list(k),as.list(v)), file = tc, ..., row.names = FALSE, col.names = FALSE)
  do.call(write.table, args[unique(names(args))])
  paste(textConnectionValue(con = tc), "\n", sep = "", collapse = "")}

default.text.input.format = native.text.input.format
default.text.output.format = native.text.output.format
#data frame conversion

flatten = function(x) unlist(list(name = as.name("name"), x))[-1]
list.to.data.frame = function(l) data.frame(lapply(data.frame(do.call(rbind,lapply(l, flatten))), unlist))
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
  list.to.data.frame(strsplit(hdfs(getcmd(match.call()), TRUE, ...)[-1], " +"))

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
  if (rmr.options.get('backend') == 'hadoop') 
    hdfs.test(e = f) 
  else file.exists(f)}

dfs.rm = function(f){
  if(rmr.options.get('backend') == 'hadoop')
    hdfs.rm(f)
  else file.remove(f)}

dfs.is.dir = function(f) { 
  if (rmr.options.get('backend') == 'hadoop') 
    hdfs.test(d = f)
  else file.info(f)['isdir']}

dfs.empty = function(f) {
  if(rmr.options.get('backend') == 'hadoop') {
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

to.dfs = function(object, file = dfs.tempfile(), text.output.format = default.text.output.format){
  if(is.data.frame(object) || is.matrix(object)) {
    object = from.data.frame(object)
  }
  tmp = tempfile()
  dfsOutput = to.dfs.path(file)
  cat(paste
       (lapply
        (object,
         function(x) {kv = if (is.keyval(x)) x else keyval(NULL, x)
                      text.output.format(kv$key, kv$val)}),
        collapse=""),
      file = tmp)
  if(rmr.options.get('backend') == 'hadoop'){
    hdfs.put(tmp, dfsOutput)
    file.remove(tmp)}
  else
    file.rename(tmp, dfsOutput)
  file
}

from.dfs = function(file, text.input.format = default.text.input.format, to.data.frame = F){
  if(rmr.options.get('backend') == 'hadoop') {
    tmp = tempfile()
    hdfs.get(to.dfs.path(file), tmp)}
  else tmp = to.dfs.path(file)
  retval = if(file.info(tmp)[1,'isdir']) {
             do.call(c,
               lapply(list.files(tmp, "part*"),
                 function(f) lapply(readLines(file.path(tmp, f)),
                            text.input.format)))}      
          else {
            lapply(readLines(tmp), text.input.format)}
  if(!to.data.frame) {
    retval}
  else{
    list.to.data.frame(retval)  }
}

# mapreduce

dfs.tempfile = function(pattern = "file", tmpdir = tempdir()) {
  fname  = tempfile(pattern, tmpdir)
  namefun = function() {fname}
  reg.finalizer(environment(namefun),
                function(e) {
                  fname = eval(expression(fname), envir = e)
                  if(Sys.getenv("mapred_task_id") != "" && dfs.exists(fname)) dfs.rm(fname)
		})
  namefun
}

dfs.managed.file = function(call, managed.dir = rmr.options.get('managed.dir')) {
  file.path(managed.dir, digest(lapply(call, eval)))}

mapreduce = function(
  input,
  output = NULL,
  map = to.map(identity),
  reduce = NULL,
  combine = NULL,
  reduce.on.data.frame = FALSE,
  input.format = NULL,
  output.format = NULL,
  text.input.format = default.text.input.format,
  text.output.format = default.text.output.format,
  tuning.parameters = list(),
  verbose = TRUE) {

  on.exit(expr = gc(), add = TRUE) #this is here to trigger cleanup of tempfiles
  if (is.null(output)) output = 
    if(rmr.options.get('depend.check'))
      dfs.managed.file(match.call())
    else
      dfs.tempfile()

  backend  =  rmr.options.get('backend')
  
  profile.nodes = rmr.options.get('profile.nodes')
    
  mr = switch(backend, hadoop = rhstream, local = mr.local, stop("Unsupported backend: ", backend))
  
  mr(map = map,
     reduce = reduce,
     reduce.on.data.frame = reduce.on.data.frame,
     combine = combine,
     in.folder = if(is.list(input)) {lapply(input, to.dfs.path)} else to.dfs.path(input),
     out.folder = to.dfs.path(output),
     profile.nodes = profile.nodes,
     input.format = input.format,
     output.format = output.format,
     text.input.format = text.input.format,
     text.output.format = text.output.format,
     tuning.parameters = tuning.parameters[[backend]],
     verbose = verbose)
  output
}

# backends

mr.local = function(map,
                    reduce,
                    reduce.on.data.frame = FALSE,
                    combine = NULL,
                    in.folder,
                    out.folder,
                    profile.nodes = FALSE,
                    input.format = NULL,
                    output.format = NULL,
                    text.input.format = default.text.input.format,
                    text.output.format = default.text.output.format,
                    tuning.parameters = list(),
                    verbose = verbose) {
  if(is.null(reduce)) reduce = function(k,vv) lapply(vv, function(v) keyval(k,v))
  map.out = do.call(c, 
                    lapply(do.call(c,
                                   lapply(in.folder, 
                                          function(x) lapply(from.dfs(x, 
                                                                      text.input.format = text.input.format),
                                                             function(y){attr(y$val, 'rmr.input') = x; y}))), 
                              function(kv) {retval = map(kv$key, kv$val)
                                            if(is.keyval(retval)) list(retval)
                                            else retval}))
  map.out = from.dfs(to.dfs(map.out))
  reduce.out = tapply(X = map.out, 
                      INDEX = sapply(keys(map.out), digest), 
                      FUN = function(x) reduce(x[[1]]$key, 
                                             if(reduce.on.data.frame) list.to.data.frame(values(x)) else values(x)),
                      simplify = FALSE)
  if(!is.keyval(reduce.out[[1]]))
    reduce.out = do.call(c, reduce.out)
  names(reduce.out) = replicate(n=length(names(reduce.out)), "")
  to.dfs(reduce.out, out.folder)}

rhstream = function(
  map,
  reduce = NULL,
  reduce.on.data.frame = F,
  combine = NULL,
  in.folder,
  out.folder, 
  line.buffer.size = 2000,
  profile.nodes = FALSE,
  cachefiles = c(),
  archives = c(),
  jarfiles = c(),
  otherparams = list(HADOOP_HOME = Sys.getenv('HADOOP_HOME'),
    HADOOP_CONF = Sys.getenv("HADOOP_CONF")),
  input.format = NULL,
  output.format = NULL,
  text.input.format = default.text.input.format,
  text.output.format = default.text.output.format,
  tuning.parameters = list(),
  verbose = TRUE,
  debug = FALSE) {
    ## prepare map and reduce executables
  lines = '#! /usr/bin/env Rscript
options(warn=1)

library(rmr)
load("rmr-local-env")
invisible(lapply(libs, function(l) library(l, character.only = T)))
'  

  map.line = 'load("rmr-map-env", envir = environment(map))
  rmr:::map.driver(map = map,
              line.buffer.size = line.buffer.size,
              text.input.format = text.input.format,
              text.output.format = if(is.null(reduce))
                                 {text.output.format}
                                 else {rmr:::default.text.output.format},
              profile = profile.nodes)'
  reduce.line  =  'load("rmr-reduce-env", envir = environment(reduce))
  rmr:::reduce.driver(reduce = reduce,
                 line.buffer.size = line.buffer.size,
                 text.input.format = rmr:::default.text.input.format,
                 text.output.format = text.output.format,
                 reduce.on.data.frame = reduce.on.data.frame,
                 profile = profile.nodes)'
  combine.line = 'load("rmr-combine-env", envir = environment(combine))
 rmr:::reduce.driver(reduce = combine,
                 line.buffer.size = line.buffer.size,
                 text.input.format = rmr:::default.text.input.format,
                 text.output.format = rmr:::default.text.output.format,
                 reduce.on.data.frame = reduce.on.data.frame,
                profile = profile.nodes)'

  map.file = tempfile(pattern = "rhstr.map")
  writeLines(c(lines,map.line), con = map.file)
  reduce.file = tempfile(pattern = "rhstr.reduce")
  writeLines(c(lines, reduce.line), con = reduce.file)
  combine.file = tempfile(pattern = "rhstr.combine")
  writeLines(c(lines, combine.line), con = combine.file)
  
  ## set up the execution environment for map and reduce
  if (!is.null(combine) && is.logical(combine) && combine) {
    combine = reduce}

  save.env = function(fun = NULL, name) {
    fun.env = file.path(tempdir(), name)
    envir = if(is.null(fun)) parent.env(environment()) else environment(fun)
    save(list = ls(all = TRUE, envir = envir), file = fun.env, envir = envir)
    fun.env}

  libs = sub("package:", "", grep("package", search(), value = T))
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
  input.format = if(is.null(input.format)){
    ' ' # default is text.input.format
  }else{
    sprintf(" -input.format %s", input.format)
  }
  output.format = if(is.null(output.format)){
    ' '}
  else {
    sprintf(" -output.format %s", output.format)
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

  cmds = make.job.conf(otherparams, pfx="-cmdenv")
  
  #debug.opts = "-mapdebug kdfkdfld -reducexdebug jfkdlfkja"
  caches = if(length(cachefiles)>0) make.cache.files(cachefiles,"-files") else " " #<0.21
  archives = if(length(archives)>0) make.cache.files(archives,"-archives") else " "
  mkjars = if(length(jarfiles)>0) make.cache.files(jarfiles,"-libjars",shorten=FALSE) else " "
  
  final.command = 
    paste(
      hadoop.command,
      paste.options(tuning.parameters),
      archives,
      caches,
      mkjars,
      input.format,
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
      "2>&1")
  if(verbose) {
    retval = system(final.command)
    if (retval != 0) stop("hadoop streaming failed with error code ", retval, "\n")}
  else{
    console.output = tryCatch(system(final.command, intern=TRUE), 
                              warning = function(e) stop(e)) 
    0
    }
}
##special jobs
 
## a sort of relational join very useful in a variety of map reduce algorithms

## to.dfs(lapply(1:10, function(i) keyval(i, i^2)), "/tmp/reljoin.left")
## to.dfs(lapply(1:10, function(i) keyval(i, i^3)), "/tmp/reljoin.right")
## equijoin(left.input="/tmp/reljoin.left", right.input="/tmp/reljoin.right", output = "/tmp/reljoin.out")
## from.dfs("/tmp/reljoin.out")

equijoin = function(
  left.input = NULL,
  right.input = NULL,
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
  stopifnot(xor(!is.null(left.input), !is.null(input) &&
                (is.null(left.input)==is.null(right.input))))
  outer = match.arg(outer)
  left.outer = outer == "left"
  right.outer = outer == "right"
  full.outer = outer == "full"
  if (is.null(left.input)) {
    left.input = input}
  mark.side =
    function(kv, isleft) keyval(kv$key, list(val = kv$val, isleft = isleft))
  is.left.side = 
    function(left.input) {
      leftin = strsplit(to.dfs.path(left.input), "/+")[[1]]
      mapin = strsplit(Sys.getenv("map_input_file"), "/+")[[1]]
      leftin = leftin[-1]
      mapin = mapin[if(mapin[1] == "hdfs:") c(-1,-2) else -1]
      all(mapin[1:length(leftin)] == leftin)}
  reduce.split =
    function(vv) tapply(lapply(vv, function(v) v$val), sapply(vv, function(v) v$isleft), identity, simplify = FALSE)
  pad.side =
    function(vv, side.outer, full.outer) if (length(vv) == 0 && (side.outer || full.outer)) c(NA) else vv
  map = if (is.null(input)) {
    function(k,v) {
      ils = switch(rmr.options.get('backend'), 
                   hadoop = is.left.side(left.input),
                   local = attr(v, 'rmr.input') == to.dfs.path(left.input),
                   stop("Unsupported backend: ", rmr.options.get('backend')))
      mark.side(if(ils) map.left(k,v) else map.right(k,v), ils)}}
  else {
    function(k,v) {
      list(mark.side(map.left(k,v), TRUE),
           mark.side(map.right(k,v), FALSE))}}
  eqj.reduce = reduce
  mapreduce(map = map,
            reduce =
            function(k, vv) {
              rs = reduce.split(vv)
              eqj.reduce(k,
                     pad.side(rs$`TRUE`, right.outer, full.outer),
                     pad.side(rs$`FALSE`, left.outer, full.outer))},
            input = c(left.input,right.input),
            output = output)}



## push a file through this to get as many partitions as possible (depending on system settings)
## data is unchanged

scatter = function(input, output = NULL)
  mapreduce(input, output, map = function(k,v) keyval(runif(1), keyval(k,v)),
            reduce = function(k,vv) vv)

##optimizer

is.mapreduce = function(x) {
  is.call(x) && x[[1]] == "mapreduce"}

mapreduce.arg = function(x, arg) {
  match.call(mapreduce,x) [[arg]]}

optimize = function(mrex) {
  mrin = mapreduce.arg(mrex, 'input')
  if (is.mapreduce(mrex) && 
    is.mapreduce(mrin) &&
    is.null(mapreduce.arg(mrin, 'output')) &&
    is.null(mapreduce.arg(mrin, 'reduce'))) {
      bquote(
        mapreduce(input =  .(mapreduce.arg(mrin, 'input')),
                  output = .(mapreduce.arg(mrex, 'output')),
                  map = .(compose.mapred)(.(mapreduce.arg(mrex, 'map')),
                                  .(mapreduce.arg(mrin, 'map'))),
                  reduce = .(mapreduce.arg(mrex, 'reduce'))))}
  else mrex }



