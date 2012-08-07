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

#strings

qw = function(...) as.character(match.call())[-1]

#data structures

make.fast.list = function(l = list()) {
  l1 = l
  l2 = list(NULL)
  i = 1
  function(els = NULL){
    if(missing(els)) c(l1, l2[!sapply(l2, is.null)]) 
    else{
      if(i + length(els) - 1 > length(l2)) {
        l1 <<- c(l1, l2[!sapply(l2, is.null)])
        i <<- 1
        l2 <<- rep(list(NULL), length(l1) + length(els))}
      l2[i:(i + length(els) - 1)] <<- els
      i <<- i + length(els)}}}

named.slice = function(x, n) x[which(names(x) == n)]

#list manip

catply = function(x, fun) do.call(c, lapply(x, fun))

#options

rmr.options = new.env(parent=emptyenv())
rmr.options$backend = "hadoop"
rmr.options$vectorized.nrows = 1000
rmr.options$profile.nodes = FALSE
rmr.options$depend.check = FALSE
#rmr.options$managed.dir = "/var/rmr/managed"

rmr.options.get = function(...) {
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
                           vectorized.nrows = NULL#, 
                           #depend.check = NULL, 
                           #managed.dir = NULL
                           ) {
  this.call = match.call()
  backend = match.arg(backend) #this doesn't do anything, fix
  lapply(names(this.call)[-1], 
         function(x) 
           assign(x, eval(this.call[[x]]), envir = rmr.options))
  as.list(rmr.options)}

# additional hadoop features, disabled for now
counter = function(group="r-stream", family, value) {
  cat(sprintf("report:counter:%s, $s, %s", 
              as.character(group), 
              as.character(family), 
              as.integer(value)), 
      stderr())
}

status = function(what) {
  cat(sprintf("report:status:%s", 
              as.character(what)), 
      stderr())
}

## could think of this as a utils section
## keyval manip
keyval = function(k, v, vectorized = FALSE) {
  kv = list(key = k, val = v)
  attr(kv, 'rmr.keyval') = TRUE
  if(vectorized) {
    attr(kv, 'rmr.vectorized') = TRUE
  }
  kv}

is.keyval = function(kv) !is.null(attr(kv, 'rmr.keyval', exact = TRUE))
is.vectorized.keyval = function(kv) !is.null(attr(kv, 'rmr.vectorized', exact = TRUE))

keyval.project = function(i) {
  list.singleton = 
    function(kv){
      if(is.keyval(kv))
        list(kv)
      else
        kv}
  function(kvl) {
    catply(list.singleton(kvl),
           function(kv) {
               if(is.vectorized.keyval(kv))
                 kv[[i]]
               else
                 list(kv[[i]])})}}
  
keys = keyval.project(1)
values = keyval.project(2)

to.list = function(x) {
  if(is.data.frame(x)) {
    .Call('dataframe_to_list', x, nrow(x), ncol(x), replicate(nrow(x), as.list(1:ncol(x)), simplify=F))}
    else {
      if(is.matrix(x))
        lapply(1:nrow(x), function(i) as.list(x[i,]))
      else
        as.list(x)}}

to.structured = 
  function(x) {
    if(is.data.frame(x) || is.matrix(x) || is.atomic(x))
      x
    else {
      if(all(sapply(x, function(x) is.null(nrow(x)))))
        unlist(x)
      else
        do.call(rbind, x)}}

from.structured = 
  function(x) {
    n = if(is.null(nrow(x))) length(x) else nrow(x)
    if(is.matrix(x)) split = split.data.frame
    split(x, ceiling((1:n)/rmr.options.get("vectorized.nrows")))}

keyval.list.to.structured=
  function(x) {
    kk = to.structured(keys(x))
    vv = to.structured(values(x))
    keyval(kk, vv, vectorized = TRUE)}

## map and reduce function generation

to.map = function(fun1, fun2 = identity) {
  if (missing(fun2)) {
    function(k, v) fun1(keyval(k, v))}
  else {
    function(k, v) keyval(fun1(k), fun2(v))}}

to.reduce = to.map

to.reduce.all = function(fun1, fun2 = identity) {
  if (missing(fun2)) {
    function(k, vv) lapply(vv, function(v) fun1(keyval(k, v)))}
  else {
    function(k, vv) lapply(vv, function(v) keyval(fun1(k), fun2(v)))}}

## mapred combinators
wrap.keyval = function(kv) {
  if(is.null(kv)) list()
  else if (is.keyval(kv)) list(kv) 
  else kv}

compose.mapred = function(mapred, map) function(k, v) {
  out = mapred(k, v)
  if (is.null(out)) NULL
  else if (is.keyval(out)) map(out$key, out$val)
  else  do.call(c, 
                lapply(out, 
                       function(x) 
                         wrap.keyval(map(x$key, x$val))))}

union.mapred = function(mr1, mr2) function(k, v) {
  out = c(wrap.keyval(mr1(k, v)), wrap.keyval(mr2(k, v)))
  if (length(out) == 0) NULL else out}



#some option formatting utils

paste.options = function(optlist) {
  optlist = unlist(sapply(optlist, function(x) if (is.logical(x)) {if(x) "" else NULL} else x))
  if(is.null(optlist)) "" 
  else paste(unlist(rbind(paste("-", names(optlist), sep = ""), optlist)), collapse = " ")}

make.input.files = function(infiles) {
  if(length(infiles) == 0) return(" ")
  paste(sapply(infiles, 
               function(r) {
                 sprintf("-input %s ", r)}), 
        collapse=" ")}

# I/O 

make.record.reader = function(mode = NULL, format = NULL, con = NULL, nrecs = 1) {
  default = make.input.format()
  if(is.null(mode)) mode = default$mode
  if(is.null(format)) format = default$format
  if(mode == "text") {
    if(is.null(con)) con = file("stdin", "r")} #not stdin() which is parsed by the interpreter
  else {
    if(is.null(con)) con = pipe("cat", "rb")}
  function() format(con, nrecs)}

make.record.writer = function(mode = NULL, format = NULL, con = NULL) {
  default = make.output.format()
  if(is.null(mode)) mode = default$mode
  if(is.null(format)) format = default$format
  if(mode == "text") {
    if(is.null(con)) con = stdout()}
  else {
    if(is.null(con)) con = pipe("cat", "wb")}
  function(k, v, vectorized) format(k, v, con, vectorized)}

IO.formats = c("text", "json", "csv", "native",
               "sequence.typedbytes")

make.input.format = function(format = native.input.format(), 
                            mode = c("binary", "text"),
                            streaming.format = NULL, ...) {
  mode = match.arg(mode)
  if(is.character(format)) {
    format = match.arg(format, IO.formats)
    switch(format, 
           text = {format = text.input.format 
                   mode = "text"}, 
           json = {format = json.input.format 
                   mode = "text"}, 
           csv = {format = csv.input.format(...) 
                  mode = "text"}, 
           native = {format = native.input.format() 
                            mode = "binary"}, 
           sequence.typedbytes = {format = typed.bytes.input.format() 
                                  mode = "binary"})}
  if(is.null(streaming.format) && mode == "binary") 
    streaming.format = "org.apache.hadoop.streaming.AutoInputFormat"
  list(mode = mode, format = format, streaming.format = streaming.format)}

make.output.format = function(format = native.output.format, 
                             mode = c("binary", "text"),
                             streaming.format = "org.apache.hadoop.mapred.SequenceFileOutputFormat", 
                             ...) {
  mode = match.arg(mode)
  if(is.character(format)) {
    format = match.arg(format, IO.formats)
    switch(format, 
           text = {format = text.output.format
                   mode = "text"
                   streaming.format = NULL},
           json = {format = json.output.format
                   mode = "text"
                   streaming.format = NULL}, 
           csv = {format = csv.output.format(...)
                  mode = "text"
                  streaming.format = NULL}, 
           native = {format = native.output.format 
                     mode = "binary"
                     streaming.format = "org.apache.hadoop.mapred.SequenceFileOutputFormat"}, 
           sequence.typedbytes = {format = typed.bytes.output.format 
                                  mode = "binary"
                                  streaming.format = "org.apache.hadoop.mapred.SequenceFileOutputFormat"})}
  mode = match.arg(mode)
  list(mode = mode, format = format, streaming.format = streaming.format)}



#output cmp
cmp = function(x, y) {
  kx  = keys(x)
  ky = keys(y)
  vx = values(x)
  vy = values(y)
  ox = order(sapply(kx, digest), sapply(vx, function(z){attr(z, "rmr.input") = NULL; digest(z)}))
  oy = order(sapply(ky, digest), sapply(vy, function(z){attr(z, "rmr.input") = NULL; digest(z)}))
  isTRUE(all.equal(kx[ox], ky[oy], check.attributes = FALSE)) &&
  isTRUE(all.equal(vx[ox], vy[oy], check.attributes = FALSE))}

#hdfs section

hdfs = function(cmd, intern, ...) {
  if (is.null(names(list(...)))) {
    argnames = sapply(1:length(list(...)), function(i) "")}
  else {
    argnames = names(list(...))}
  system(paste(hadoop.cmd(), " dfs -", cmd, " ", 
              paste(
                apply(cbind(argnames, list(...)), 1, 
                  function(x) paste(
                    if(x[[1]] == "") {""} else {"-"}, 
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

#this returns a character matrix, individual cmds may benefit from additional transformations
hdfs.match.out = function(...) {
  oldwarn = options("warn")[[1]]
  options(warn = -1)
  retval = do.call(rbind, strsplit(hdfs(getcmd(match.call()), TRUE, ...), " +")) 
  options(warn = oldwarn)
  retval}

mkhdfsfun = function(hdfscmd, out)
  eval(parse(text = paste ("hdfs.", hdfscmd, " = hdfs.match.", if(out) "out" else "sideeffect", sep = "")), 
       envir = parent.env(environment()))

for (hdfscmd in c("ls", "lsr", "df", "du", "dus", "count", "cat", "text", "stat", "tail", "help")) 
  mkhdfsfun(hdfscmd, TRUE)

for (hdfscmd in c("mv", "cp", "rm", "rmr", "expunge", "put", "copyFromLocal", "moveFromLocal", "get", "getmerge", 
                 "copyToLocal", "moveToLocal", "mkdir", "setrep", "touchz", "test", "chmod", "chown", "chgrp"))
  mkhdfsfun(hdfscmd, FALSE)

pretty.hdfs.ls = function(...) {
  ls.out = hdfs.ls(...)
  crud = grep("Found", ls.out[,1])
  if(length(crud) > 0)
    ls.out = ls.out[-crud,]
  if(class(ls.out) == "character") ls.out = t(ls.out)
  df = as.data.frame(ls.out,stringsAsFactors=F)
  names(df) = c("mode", "links", "owner", "group", "size", "last.modified.date", "last.modified.time", "path")
  df$links = as.numeric(sapply(as.character(df$links), function(x) if (x=="-") 0 else x))
  df$size = as.numeric(as.character(df$size))
  df}

# backend independent dfs section
part.list = function(fname) {
  if(rmr.options.get('backend') == "local") fname
  else {
    if(dfs.is.dir(fname))
      pretty.hdfs.ls(paste(fname, "part*", sep = "/"))$path
    else fname}}

dfs.exists = function(f) {
  if (rmr.options.get('backend') == 'hadoop') 
    hdfs.test(e = f) 
  else file.exists(f)}

dfs.rm = function(f) {
  if(rmr.options.get('backend') == 'hadoop')
    hdfs.rm(f)
  else file.remove(f)}

dfs.is.dir = function(f) { 
  if (rmr.options.get('backend') == 'hadoop') 
    hdfs.test(d = f)
  else file.info(f)['isdir']}

dfs.empty = function(f) {
  f = to.dfs.path(f)
  if(rmr.options.get('backend') == 'hadoop') {
    if(dfs.is.dir(f)) {
      all(
        lapply(
          part.list(f),
          function(x) hdfs.test(z = x)))}
    else {hdfs.test(z = f)}}
  else file.info(f)['size'] == 0}

# dfs bridge

to.dfs.path = function(input) {
  if (is.character(input)) {
    input}
  else {
    if(is.function(input)) {
      input()}}}

to.dfs = function(object, output = dfs.tempfile(), format = "native") {
  obj.class = class(object)
  if(is.data.frame(object) || is.matrix(object) || is.atomic(object))
    object = from.structured(object)
  if (!is.list(object)) stop(obj.class, " is not supported by to.dfs")
  tmp = tempfile()
  dfsOutput = to.dfs.path(output)
  if(is.character(format)) format = make.output.format(format)
  
  write.file = 
    function(obj, f) {
      con = file(f, if(format$mode == "text") "w" else "wb")
        record.writer = make.record.writer(format$mode, 
                                           format$format, 
                                           con)
      if(is.vectorized.keyval(obj))
        record.writer(obj$key, obj$val, TRUE)
      else
        lapply(obj, 
               function(x) {
                 kv = if(is.keyval(x)) x else keyval(NULL, x)
                 record.writer(kv$key, kv$val, FALSE)})
      close(con)}
    
  write.file(object, tmp)      
  if(rmr.options.get('backend') == 'hadoop') {
    if(format$mode == "binary")
      system(paste(hadoop.streaming(),  "loadtb", dfsOutput, "<", tmp))
    else  hdfs.put(tmp, dfsOutput)}
  else file.copy(tmp, dfsOutput)
  file.remove(tmp)
  output}

from.dfs = function(input, format = "native", to.data.frame = FALSE, vectorized = FALSE, structured = FALSE) {
  if(is.logical(vectorized)) nrecs = if(vectorized) rmr.options$vectorized.nrows else 1
  else nrecs = vectorized 
  
  read.file = function(f) {
    con = file(f, if(format$mode == "text") "r" else "rb")
    record.reader = make.record.reader(format$mode, format$format, con, nrecs)
    retval = make.fast.list()
    rec = record.reader()
    while(!is.null(rec)) {
      retval(if(is.keyval(rec)) list(rec) else rec)
      rec = record.reader()}
    close(con)
    retval()}
  
  dumptb = function(src, dest){
    lapply(src, function(x) system(paste(hadoop.streaming(), "dumptb", x, ">>", dest)))}
  
  getmerge = function(src, dest) {
    on.exit(unlink(tmp))
    tmp = tempfile()
    lapply(src, function(x) {
      hdfs.get(as.character(x), tmp)
      system(paste('cat', tmp, '>>' , dest))
      unlink(tmp)})
    dest}
  
  fname = to.dfs.path(input)
  if(is.character(format)) format = make.input.format(format)
  if(rmr.options.get("backend") == "hadoop") {
    tmp = tempfile()
    if(format$mode == "binary") dumptb(part.list(fname), tmp)
    else getmerge(part.list(fname), tmp)}
  else
    tmp = fname
  retval = read.file(tmp)
  if(rmr.options.get("backend") == "hadoop") unlink(tmp)
  if(structured) 
    keyval.list.to.structured(retval)
  else {
    if(is.logical(vectorized) && vectorized)
      keyval(do.call(c, lapply(retval,keys)), do.call(c, lapply(retval, values)), vectorized = TRUE)
    else retval}}

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
  input.format = "native", 
  output.format = "native", 
  vectorized = list(map = FALSE, reduce = FALSE),
  structured = list(map = FALSE, reduce = FALSE),
  backend.parameters = list(), 
  verbose = TRUE) {

  on.exit(expr = gc(), add = TRUE) #this is here to trigger cleanup of tempfiles
  if (is.null(output)) output = 
    if(rmr.options.get('depend.check'))
      dfs.managed.file(match.call())
    else
      dfs.tempfile()
  if(is.character(input.format)) input.format = make.input.format(input.format)
  if(is.character(output.format)) output.format = make.output.format(output.format)
  if(is.logical(vectorized)) vectorized = list(map = vectorized, reduce = vectorized)
  if(is.logical(vectorized$map)){
    vectorized$map = if (vectorized$map) rmr.options$vectorized.nrows else 1}
  if(is.logical(structured)) structured = list(map = structured, reduce = structured)
  structured$map = !is.null(structured$map) && structured$map && (vectorized$map != 1)
  if(is.null(structured$reduce)) structured$reduce = FALSE
  
  backend  =  rmr.options.get('backend')
  
  profile.nodes = rmr.options.get('profile.nodes')
    
  mr = switch(backend, hadoop = rhstream, local = mr.local, stop("Unsupported backend: ", backend))
  
  mr(map = map, 
     reduce = reduce, 
     combine = combine, 
     in.folder = if(is.list(input)) {lapply(input, to.dfs.path)} else to.dfs.path(input), 
     out.folder = to.dfs.path(output), 
     profile.nodes = profile.nodes, 
     input.format = input.format, 
     output.format = output.format, 
     vectorized = vectorized,
     structured = structured,
     backend.parameters = backend.parameters[[backend]], 
     verbose = verbose)
  output
}

# backends

#local

mr.local = function(map, 
                    reduce, 
                    combine, 
                    in.folder, 
                    out.folder, 
                    profile.nodes, 
                    input.format, 
                    output.format, 
                    vectorized,
                    structured,
                    backend.parameters, 
                    verbose = verbose) {
  if(is.null(reduce)) reduce = function(k, vv) lapply(vv, function(v) keyval(k, v))
  
  apply.map =   
    function(kv) {
      retval = 
        if(structured$map && vectorized$map) 
          map(to.structured(kv$key), to.structured(kv$val))
        else map(kv$key,kv$val)

      if(is.keyval(retval) && !is.vectorized.keyval(retval)) list(retval)
      else {
        if(is.vectorized.keyval(retval)){
          kr = keys(retval)
          vr = values(retval)
          lapply(seq_along(kr), function(i) keyval(kr[[i]], vr[[i]]))}
        else retval}}
  get.data =
    function(fname) {
      in.data = from.dfs(fname, format = input.format, vectorized = vectorized$map)
      if(!isTRUE(vectorized$map)) {
        lapply(in.data, function(rec) {attr(rec$val, 'rmr.input') = fname; rec})}
      else {
        attr(in.data$val, 'rmr.input') = fname
        list(in.data)}}
  map.out = 
    catply(
      catply(
        in.folder,
        get.data),
      apply.map)
  map.out = from.dfs(to.dfs(map.out))
  reduce.out = as.list(tapply(X = map.out, 
                      INDEX = sapply(keys(map.out), digest), 
                      FUN = function(x) reduce(x[[1]]$key, 
                                             if(structured$reduce) to.structured(values(x)) else values(x)), 
                      simplify = FALSE))
  if(!is.keyval(reduce.out[[1]]))
    reduce.out = do.call(c, reduce.out)
  names(reduce.out) = replicate(n=length(names(reduce.out)), "")
  to.dfs(reduce.out, out.folder, format = output.format)}

#hadoop
## loops section, or what runs on the nodes

activate.profiling = function() {
  dir = file.path("/tmp/Rprof", Sys.getenv('mapred_job_id'), Sys.getenv('mapred_tip_id'))
  dir.create(dir, recursive = T)
  Rprof(file.path(dir, paste(Sys.getenv('mapred_task_id'), Sys.time())))}
  
close.profiling = function() Rprof(NULL)


map.loop = function(map, record.reader, record.writer, structured, profile) {
  if(profile) activate.profiling()
  kv = record.reader()
  while(!is.null(kv)) { 
    out = map(if(structured) to.structured(kv$key) else kv$key, 
              if(structured) to.structured(kv$val) else kv$val)
    if(!is.null(out)) {
      if (is.keyval(out)) {record.writer(out$key, out$val, is.vectorized.keyval(out))}
      else {lapply(out, function(o) record.writer(o$key, o$val, is.vectorized.keyval(o)))}}
    kv = record.reader()}
  if(profile) close.profiling()
  invisible()}

list.cmp = function(ll, e) sapply(ll, function(l) isTRUE(all.equal(e, l, check.attributes = FALSE)))
## using isTRUE(all.equal(x)) because identical() was too strict, but on paper it should be it

reduce.loop = function(reduce, record.reader, record.writer, structured, profile) {
  reduce.flush = function(current.key, vv) {
    out = reduce(current.key, 
                 if(structured) {
                   to.structured(vv)}
                 else {vv})
    if(!is.null(out)) {
      if(is.keyval(out)) {record.writer(out$key, out$val, is.vectorized.keyval(out))}
      else {lapply(out, function(o) record.writer(o$key, o$val, is.vectorized.keyval(o)))}}}
  if(profile) activate.profiling()
  kv = record.reader()
  current.key = kv$key
  vv = make.fast.list()
  while(!is.null(kv)) {
    if(identical(kv$key, current.key)) vv(if(is.vectorized.keyval(kv)) kv$val else list(kv$val))
    else {
      reduce.flush(current.key, vv())
      current.key = kv$key
      vv = make.fast.list(if(is.vectorized.keyval(kv)) kv$val else list(kv$val))}
    kv = record.reader()}
  if(length(vv()) > 0) reduce.flush(current.key, vv())
  if(profile) close.profiling()
  invisible()
}

# the main function for the hadoop backend

hadoop.cmd = function() {
  hadoop_cmd = Sys.getenv("HADOOP_CMD")
  if( hadoop_cmd == "") {
    hadoop_home = Sys.getenv("HADOOP_HOME")
    if(hadoop_home == "") stop("Please make sure that the env. variable HADOOP_CMD or HADOOP_HOME are set")
      file.path(hadoop_home, "bin", "hadoop")}
  else hadoop_cmd}
  
hadoop.streaming = function() {
  hadoop_streaming = Sys.getenv("HADOOP_STREAMING")
  if(hadoop_streaming == ""){
    hadoop_home = Sys.getenv("HADOOP_HOME")
    if(hadoop_home == "") stop("Please make sure that the env. variable HADOOP_STREAMING or HADOOP_HOME are set")
    stream.jar = list.files(path=sprintf("%s/contrib/streaming", hadoop_home), pattern="jar$", full.names = TRUE)
    sprintf("%s jar %s ", hadoop.cmd(), stream.jar)}
  else sprintf("%s jar %s ", hadoop.cmd(), hadoop_streaming)}
  
rhstream = function(
  map, 
  reduce, 
  combine, 
  in.folder, 
  out.folder, 
  profile.nodes, 
  input.format, 
  output.format, 
  vectorized,
  structured,
  backend.parameters, 
  verbose = TRUE, 
  debug = FALSE) {
    ## prepare map and reduce executables
  lines = 'options(warn=1)

library(rmr)
load("rmr-local-env")
load("rmr-global-env")
invisible(lapply(libs, function(l) require(l, character.only = T)))
'  
  map.line = '  rmr:::map.loop(map = map, 
              record.reader = rmr:::make.record.reader(input.format$mode, 
                                                       input.format$format,
                                                       nrecs = vectorized$map), 
              record.writer = if(is.null(reduce)) {
                                rmr:::make.record.writer(output.format$mode, 
                                                   output.format$format)}
                              else {
                                rmr:::make.record.writer()}, 
              structured$map && (vectorized$map > 1),
              profile = profile.nodes)'
  reduce.line  =  '  rmr:::reduce.loop(reduce = reduce, 
                 record.reader = rmr:::make.record.reader(), 
                 record.writer = rmr:::make.record.writer(output.format$mode, 
                                                    output.format$format), 
                 structured = structured$reduce, 
                 profile = profile.nodes)'
  combine.line = '  rmr:::reduce.loop(reduce = combine, 
                 record.reader = rmr:::make.record.reader(), 
                 record.writer = rmr:::make.record.writer(), 
                 structured = structured$reduce,             
                profile = profile.nodes)'

  map.file = tempfile(pattern = "rhstr.map")
  writeLines(c(lines, map.line), con = map.file)
  reduce.file = tempfile(pattern = "rhstr.reduce")
  writeLines(c(lines, reduce.line), con = reduce.file)
  combine.file = tempfile(pattern = "rhstr.combine")
  writeLines(c(lines, combine.line), con = combine.file)
  
  ## set up the execution environment for map and reduce
  if (!is.null(combine) && is.logical(combine) && combine) {
    combine = reduce}
  
  save.env = function(fun = NULL, name) {
    fun.env = file.path(tempdir(), name)
    envir = 
      if(is.null(fun)) parent.env(environment()) else {
        if (is.function(fun)) environment(fun)
        else fun}
    save(list = ls(all.names = TRUE, envir = envir), file = fun.env, envir = envir)
    fun.env}

  libs = sub("package:", "", grep("package", search(), value = T))
  image.cmd.line = paste("-file",
                         c(save.env(name = "rmr-local-env"),
                           save.env(.GlobalEnv, "rmr-global-env")),
                         collapse = " ")
  ## prepare hadoop streaming command
  hadoop.command = hadoop.streaming()
  input =  make.input.files(in.folder)
  output = if(!missing(out.folder)) sprintf("-output %s", out.folder) else " "
  input.format.opt = if(is.null(input.format$streaming.format)) {
    ' ' # default is TextInputFormat
  }else {
    sprintf(" -inputformat %s", input.format$streaming.format)
  }
  output.format.opt = if(is.null(output.format$streaming.format)) {
    ' '}
  else {
    sprintf(" -outputformat %s", output.format$streaming.format)
  }
  stream.map.input =
    if(input.format$mode == "binary") {
      " -D stream.map.input=typedbytes"}
    else {''}
  stream.map.output = 
    if(is.null(reduce) && output.format$mode == "text") "" 
    else   " -D stream.map.output=typedbytes"
  stream.reduce.input = " -D stream.reduce.input=typedbytes"
  stream.reduce.output = 
    if(output.format$mode == "binary") " -D stream.reduce.output=typedbytes"
    else ''
  stream.mapred.io = paste(stream.map.input,
                           stream.map.output,
                           stream.reduce.input,
                           stream.reduce.output)
  mapper = sprintf('-mapper "Rscript %s" ', tail(strsplit(map.file, "/")[[1]], 1))
  m.fl = sprintf("-file %s ", map.file)
  if(!is.null(reduce) ) {
      reducer = sprintf('-reducer "Rscript %s" ', tail(strsplit(reduce.file, "/")[[1]], 1))
      r.fl = sprintf("-file %s ", reduce.file)}
  else {
      reducer=" ";r.fl = " "}
  if(!is.null(combine) && is.function(combine)) {
    combiner = sprintf('-combiner "Rscript %s" ', tail(strsplit(combine.file, "/")[[1]], 1))
    c.fl = sprintf("-file %s ", combine.file)}
  else {
    combiner = " "
    c.fl = " "}
  if(is.null(reduce) && 
    !is.element("mapred.reduce.tasks",
                sapply(strsplit(as.character(named.slice(backend.parameters, 'D')), '='), 
                       function(x)x[[1]])))
    backend.parameters = append(backend.parameters, list(D='mapred.reduce.tasks=0'))
    #debug.opts = "-mapdebug kdfkdfld -reducexdebug jfkdlfkja"
  
  final.command =
    paste(
      hadoop.command, 
      stream.mapred.io,  
      paste.options(backend.parameters), 
      input, 
      output, 
      mapper, 
      combiner,
      reducer, 
      image.cmd.line, 
      m.fl, 
      r.fl, 
      c.fl,
      input.format.opt, 
      output.format.opt, 
      "2>&1")
  if(verbose) {
    retval = system(final.command)
    if (retval != 0) stop("hadoop streaming failed with error code ", retval, "\n")}
  else {
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
                                       function(vr) reduce.all(k, vl, vr)))), 
  reduce.all  = function(k, vl, vr) keyval(k, list(left = vl, right = vr)))
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
      mapin = mapin[if(is.element(mapin[1], c("hdfs:", "maprfs:"))) c(-1, -2) else -1]
      all(mapin[1:length(leftin)] == leftin)}
  reduce.split =
    function(vv) tapply(lapply(vv, function(v) v$val), sapply(vv, function(v) v$isleft), identity, simplify = FALSE)
  pad.side =
    function(vv, side.outer, full.outer) if (length(vv) == 0 && (side.outer || full.outer)) c(NA) else vv
  map = if (is.null(input)) {
    function(k, v) {
      ils = switch(rmr.options.get('backend'), 
                   hadoop = is.left.side(left.input), 
                   local = attr(v, 'rmr.input') == to.dfs.path(left.input), 
                   stop("Unsupported backend: ", rmr.options.get('backend')))
      mark.side(if(ils) map.left(k, v) else map.right(k, v), ils)}}
  else {
    function(k, v) {
      list(mark.side(map.left(k, v), TRUE), 
           mark.side(map.right(k, v), FALSE))}}
  eqj.reduce = reduce
  mapreduce(map = map, 
            reduce =
            function(k, vv) {
              rs = reduce.split(vv)
              eqj.reduce(k, 
                     pad.side(rs$`TRUE`, right.outer, full.outer), 
                     pad.side(rs$`FALSE`, left.outer, full.outer))}, 
            input = c(left.input, right.input), 
            output = output)}


