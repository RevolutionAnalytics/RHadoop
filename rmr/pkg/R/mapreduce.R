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
                           depend.check = NULL, 
                           managed.dir = NULL) {
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

make.job.conf = function(m, pfx) {
  N = names(m)
  if(length(m) == 0) return(" ")
  paste(unlist(lapply(1:length(N), 
                      function(i) {
                        sprintf("%s %s=%s ", pfx, N[[i]], as.character(m[[i]]))})), 
        collapse = " ")}

make.cache.files = function(caches, pfx, shorten = TRUE) {
  if(length(caches) == 0) return(" ")
  sprintf("%s %s", pfx, paste(sapply(caches, 
                                    function(r) {
                                      if(shorten) {
                                        path.leaf = tail(strsplit(r, "/")[[1]], 1)
                                        sprintf("'%s#%s'", r, path.leaf)
                                      }else {
                                        sprintf("'%s'", r)}}), 
                             collapse = ", "))}

make.input.files = function(infiles) {
  if(length(infiles) == 0) return(" ")
  paste(sapply(infiles, 
               function(r) {
                 sprintf("-input %s ", r)}), 
        collapse=" ")}

# I/O 

make.record.reader = function(mode = c("text", "binary"), 
                              format = native.input.format, 
                              con = NULL) {
  mode = match.arg(mode)
  if(mode == "text") {
    if(is.null(con)) con = file("stdin", "r") #not stdin() which is parsed by the interpreter
    function() {
      line = readLines(con, 1)
      if(length(line) == 0) NULL
      else format(line)}}
  else {
    if(is.null(con)) con = pipe("cat", "rb")
    function() format(con)}}

make.record.writer = function(mode = c("text", "binary"), 
                              format = native.output.format, 
                              con = NULL) {
  mode = match.arg(mode)
  if(mode == "text") {
    if(is.null(con)) con = stdout()
    function(k, v) writeLines(format(k, v), con)}
  else {
    if(is.null(con)) con = pipe("cat", "wb")
    function(k, v) {
      format(k, v, con)}}}

make.input.specs = function(format = native.input.format, 
                            mode = c("text", "binary"), 
                            streaming.input.format = NULL, ...) {
  mode = match.arg(mode)
  if(is.character(format)) {
    format = match.arg(format, c("text", "json", "csv", "native", "typedbytes", "sequenceastext"))
    switch(format, 
           text = {format = text.input.format; 
                    mode = "text"}, 
                  json = {format = json.input.format; 
                    mode = "text"}, 
                  csv = {format = csv.input.format(...); 
                    mode = "text"}, 
                  native = {format = native.input.format; 
                    mode = "text"}, 
                  typedbytes = {format = typed.bytes.input.format; 
                    mode = "binary"}, 
                  sequenceastext = {format = sequence.input.format; 
                    mode = "text";
                    streaming.input.format = "org.apache.hadoop.mapred.SequenceFileAsTextInputFormat"})}
  if(is.null(streaming.input.format) && mode == "binary") 
    streaming.input.format = "org.apache.hadoop.streaming.AutoInputFormat"
  list(mode = mode, format = format, streaming.input.format = streaming.input.format)}

make.output.specs = function(mode = c("text", "binary"), 
                       format = native.output.format, 
                       streaming.output.format = NULL, ...) {
  mode = match.arg(mode)
  if(is.character(format)) {
  format = match.arg(format, c("text", "json", "csv", "native", "typedbytes", "sequence"))
  switch(format, 
         text = {format = text.output.format; 
                  mode = "text";
                  streaming.output.format = NULL}, 
                json = {format = json.output.format; 
                  mode = "text";
                  streaming.output.format = NULL}, 
                csv = {format = csv.output.format(...); 
                  mode = "text";
                  streaming.output.format = NULL}, 
                native = {format = native.output.format; 
                  mode = "text";
                  streaming.output.format = NULL}, 
                typedbytes = {format = typed.bytes.output.format; 
                  mode = "binary";
                  streaming.output.format = NULL}, 
                sequence = {format = sequence.output.format; 
                  mode = "text";
                  streaming.output.format = "SequenceFileAsText"})}
  mode = match.arg(mode)
  list(mode = mode, format = format, streaming.output.format = streaming.output.format)}

native.input.format = function(line) {
  if (length(line) == 0) NULL
  else {
    x = strsplit(line, "\t")[[1]]
    de = function(x) unserialize(charToRaw(gsub("\\\\n", "\n", x)))
    keyval(de(x[1]), de(x[2]))}}

native.output.format = function(k, v) {
  ser = function(x) gsub("\n", "\\\\n", rawToChar(serialize(x, ascii=T, conn = NULL)))
  paste(ser(k), ser(v), sep = "\t")}
  
json.input.format = function(line) {
  decodeString = function(s) gsub("\\\\n", "\\\n", gsub("\\\\t", "\\\t", s))
  x =  strsplit(line, "\t")[[1]]
  keyval(fromJSON(decodeString(x[1]), asText = TRUE), 
         fromJSON(decodeString(x[2]), asText = TRUE))}

json.output.format = function(k, v) {
  encodeString = function(s) gsub("\\\n", "\\\\n", gsub("\\\t", "\\\\t", s))
  paste(encodeString(toJSON(k, collapse = "")), encodeString(toJSON(v, collapse = "")), sep = "\t")}

text.input.format = function(line) {keyval(NULL, line)}
csv.input.format = function(key = 1, ...) function(line) {
  tc = textConnection(line)
  df = tryCatch(read.table(file = tc, header = FALSE, ...), 
                error = 
                  function(e) {
                    if (e$message == "no lines available in input") 
                      write(x="No data in this line", file=stderr()) 
                    else stop(e)
                    NULL})
  close(tc)
  keyval(df[, key], df[, -key])}

text.output.format = function(k, v) paste(k, v, collapse = "", sep = "\t")
csv.output.format = function(...) function(k, v) {
  tc = textConnection(object = NULL, open = "w")
  args = list(x = c(as.list(k), as.list(v)), file = tc, ..., row.names = FALSE, col.names = FALSE)
  do.call(write.table, args[unique(names(args))])
  paste(textConnectionValue(con = tc), sep = "", collapse = "")}

typed.bytes.reader = function (con) {
  r = function(...) {
    if(is.raw(con)) 
      con = rawConnection(con, open = "r") 
    readBin(con, endian = "big", signed = TRUE, ...)}
  read.code = function() r(what = "integer", n = 1, size = 1)
  read.length = function() r(what = "integer", n= 1, size = 4)
  tbr = function() typed.bytes.reader(con) 
  type.code = read.code()
  if(length(type.code) > 0) {
    list(switch(1 + type.code, 
           r("raw", n = read.length()), #0 
           r("raw"),                    #1
           r("logical", size = 1),      #2
           r("integer", size = 4),      #3
           r("integer", size = 8),      #4
           r("numeric", size = 4),      #5
           r("numeric", size = 8),      #6
           readChar(con, nchars = read.length(), useBytes=TRUE), #7 
           lapply(1:read.length(), function(i) typed.bytes.reader(con)), #8 
           stop("not implemented yet"), #9
           lapply(1:(read.length()/2), function(i) keyval(typed.bytes.reader(con), 
                                                               typed.bytes.reader(con)))))} #10
  else NULL}

typed.bytes.writer = function(value, con) {
  w = function(x, size = NA_integer_) writeBin(x, con, size = size, endian = "big")
  write.code = function(x) w(as.integer(x), size = 1)
  write.length = function(x) w(as.integer(x), size = 4)
  tbw = function(x) typed.bytes.writer(x, con)
  if(is.list(value) && all(sapply(value, is.keyval))) {
    write.code(10)
    write.length(2*length(value))
    lapply(value, function(kv) {lapply(kv, tbw)})}
  else {
    if(length(value) == 1) {
      switch(class(value), 
             raw = {write.code(1); w(value)}, 
             logical = {write.code(2); w(value, size = 1)}, 
             integer = {write.code(3); w(value)}, 
             #doesn't happen in R integer = {write.code(4); w(value)}, 
             #doesn't happen in R numeric = {write.code(5); w(value)}, 
             numeric = {write.code(6); w(value)}, 
             character = {write.code(7); write.length(nchar(value)); writeChar(value, con, eos = NULL)}, 
             stop("not implemented yet"))}
    else {
      switch(class(value), 
             raw = {write.code(0); write.length(length(value)); w(value)}, 
             {write.code(8); write.length(length(value)); lapply(value, tbw)})}}
  TRUE}
    
typed.bytes.input.format = function(con) {
  key = typed.bytes.reader(con)
  val = typed.bytes.reader(con)
  if(is.null(key) || is.null(val)) NULL
  else keyval(key[[1]],val[[1]])}

typed.bytes.output.format = function(k, v, con) {
  typed.bytes.writer(k, con)
  typed.bytes.writer(v, con)}

#data frame conversiondefault.input.f

flatten = function(x) unlist(list(name = as.name("name"), x))[-1]
list.to.data.frame = function(l) data.frame(lapply(data.frame(do.call(rbind, lapply(l, flatten))), unlist))
from.data.frame = function(df, keycol = 1) lapply(1:dim(df)[[1]], function(i) keyval(df[i, keycol], df[i, ] ))

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

hdfs.match.out = function(...)
  list.to.data.frame(strsplit(hdfs(getcmd(match.call()), TRUE, ...)[-1], " +"))

mkhdfsfun = function(hdfscmd, out)
  eval(parse(text = paste ("hdfs.", hdfscmd, " = hdfs.match.", if(out) "out" else "sideeffect", sep = "")), 
       envir = parent.env(environment()))

for (hdfscmd in c("ls", "lsr", "df", "du", "dus", "count", "cat", "text", "stat", "tail", "help")) 
  mkhdfsfun(hdfscmd, TRUE)

for (hdfscmd in c("mv", "cp", "rm", "rmr", "expunge", "put", "copyFromLocal", "moveFromLocal", "get", "getmerge", 
                 "copyToLocal", "moveToLocal", "mkdir", "setrep", "touchz", "test", "chmod", "chown", "chgrp"))
  mkhdfsfun(hdfscmd, FALSE)

# backend independent dfs section
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

to.dfs = function(object, output = dfs.tempfile(), output.specs = make.output.specs()) {
  if(is.data.frame(object) || is.matrix(object)) {
    object = from.data.frame(object)
  }
  tmp = tempfile()
  dfsOutput = to.dfs.path(output)
  
  write.file = 
    function(obj, f) {
      con = file(f, if(output.specs$mode == "text") "w" else "wb")
        record.writer = make.record.writer(output.specs$mode, 
                                           output.specs$format, 
                                           con)
        lapply(obj, 
               function(x) {
                 kv = if(is.keyval(x)) x else keyval(NULL, x)
                 record.writer(kv$key, kv$val)})
      close(con)}
    
  write.file(object, tmp)      
  if(rmr.options.get('backend') == 'hadoop') {
    hdfs.put(tmp, dfsOutput)
    file.remove(tmp)}
  else
    file.rename(tmp, dfsOutput)
  output}

from.dfs = function(input, input.specs = make.input.specs(), to.data.frame = FALSE) {
  if(rmr.options.get('backend') == 'hadoop') {
    tmp = tempfile()
    hdfs.get(to.dfs.path(input), tmp)}
  else tmp = to.dfs.path(input)
  read.file = 
    function(f) {
      con = file(f, if(input.specs$mode == "text") "r" else "rb")
      record.reader = make.record.reader(input.specs$mode, input.specs$format, con)
      retval = list()
      rec = record.reader()
      i = 1
      while(!is.null(rec)) {
        logi = log2(i)
        if(round(logi) == logi) retval = c(retval, rep(list(NULL), length(retval)))
        retval[[i]] = rec
        rec = record.reader()
        i = i + 1}
      close(con)
      retval[!sapply(retval, is.null)]}

  retval = if(file.info(tmp)[1, 'isdir']) {
             do.call(c, 
               lapply(list.files(tmp, "part*"), 
                      function(f) read.file(file.path(tmp, f))))}
          else {read.file(tmp)}
  if(to.data.frame) list.to.data.frame(retval)
  else retval}

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
  input.specs = make.input.specs(), 
  output.specs = make.output.specs(), 
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
     input.specs = input.specs, 
     output.specs = output.specs, 
     tuning.parameters = tuning.parameters[[backend]], 
     verbose = verbose)
  output
}

# backends

#local

mr.local = function(map, 
                    reduce, 
                    reduce.on.data.frame, 
                    combine, 
                    in.folder, 
                    out.folder, 
                    profile.nodes, 
                    input.specs, 
                    output.specs, 
                    tuning.parameters, 
                    verbose = verbose) {
  if(is.null(reduce)) reduce = function(k, vv) lapply(vv, function(v) keyval(k, v))
  map.out = do.call(c, 
                    lapply(do.call(c, 
                                   lapply(in.folder, 
                                          function(x) lapply(from.dfs(x, 
                                                                      input.specs = input.specs), 
                                                             function(y) {attr(y$val, 'rmr.input') = x; y}))), 
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
  to.dfs(reduce.out, out.folder, output.specs = output.specs)}

#hadoop
## drivers section, or what runs on the nodes

activate.profiling = function() {
  dir = file.path("/tmp/Rprof", Sys.getenv('mapred_job_id'), Sys.getenv('mapred_tip_id'))
  dir.create(dir, recursive = T)
  Rprof(file.path(dir, paste(Sys.getenv('mapred_task_id'), Sys.time())))}
  
close.profiling = function() Rprof(NULL)


map.driver = function(map, record.reader, record.writer, profile) {
  if(profile) activate.profiling()
  kv = record.reader()
  while(!is.null(kv)) { 
    out = map(kv$key, kv$val)
    if(!is.null(out)) {
      if (is.keyval(out)) {record.writer(out$key, out$val)}
      else {lapply(out, function(o) record.writer(o$key, o$val))}}
    kv = record.reader()}
  if(profile) close.profiling()
  invisible()}

list.cmp = function(ll, e) sapply(ll, function(l) isTRUE(all.equal(e, l)))
## using isTRUE(all.equal(x)) because identical() was too strict, but on paper it should be it

reduce.driver = function(reduce, record.reader, record.writer, reduce.on.data.frame, profile) {
  reduce.flush = function(current.key, vv) {
    out = reduce(current.key, 
                 if(reduce.on.data.frame) {
                   list.to.data.frame(vv)}
                 else {vv})
    if(!is.null(out)) {
      if(is.keyval(out)) {record.writer(out$key, out$val)}
      else {lapply(out, function(o) record.writer(o$key, o$val))}}}
  if(profile) activate.profiling()
  kv = record.reader()
  current.key = kv$key
  vv = list()
  i = 1
  while(!is.null(kv)) {
    if(identical(kv$key, current.key)) {
      logi = log2(i)
      if(round(logi)==logi) vv = c(vv, rep(list(NULL), length(vv)))
      vv[[i]]  = kv$val
      i = i + 1
    }
    else {
      reduce.flush(current.key, 
                   vv[!sapply(vv, is.null)])
      current.key = kv$key
      vv = list(kv$val)
      i = 2
    }
    kv = record.reader()         
  }
  reduce.flush(current.key, vv[!sapply(vv, is.null)])
  if(profile) close.profiling()
  invisible()
}

# the main function for the hadoop backend
rhstream = function(
  map, 
  reduce, 
  reduce.on.data.frame, 
  combine, 
  in.folder, 
  out.folder, 
  profile.nodes, 
  cachefiles = NULL, 
  archives = NULL, 
  jarfiles = NULL, 
  otherparams = list(HADOOP_HOME = Sys.getenv('HADOOP_HOME'), 
    HADOOP_CONF = Sys.getenv("HADOOP_CONF")), 
  input.specs, 
  output.specs, 
  tuning.parameters, 
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
              record.reader = rmr:::make.record.reader(input.specs$mode, 
                                                 input.specs$format), 
              record.writer = if(is.null(reduce)) {
                                rmr:::make.record.writer(output.specs$mode, 
                                                   output.specs$format)}
                              else {
                                rmr:::make.record.writer()}, 
              profile = profile.nodes)'
  reduce.line  =  'load("rmr-reduce-env", envir = environment(reduce))
  rmr:::reduce.driver(reduce = reduce, 
                 record.reader = rmr:::make.record.reader(), 
                 record.writer = rmr:::make.record.writer(output.specs$mode, 
                                                    output.specs$format), 
                 reduce.on.data.frame = reduce.on.data.frame, 
                 profile = profile.nodes)'
  combine.line = 'load("rmr-combine-env", envir = environment(combine))
 rmr:::reduce.driver(reduce = combine, 
                 record.reader = rmr:::make.record.reader(), 
                 record.writer = rmr:::make.record.writer(), 
                 reduce.on.data.frame = reduce.on.data.frame, 
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
  stream.jar = list.files(path=sprintf("%s/contrib/streaming", hadoopHome), pattern="jar$", full=TRUE)
  hadoop.command = sprintf("%s/hadoop jar %s ", hadoopBin, stream.jar)
  input =  make.input.files(in.folder)
  output = if(!missing(out.folder)) sprintf("-output %s", out.folder) else " "
  input.format = if(is.null(input.specs$streaming.input.format)) {
    ' ' # default is TextInputFormat
  }else {
    sprintf(" -inputformat %s", input.specs$streaming.input.format)
  }
  output.format = if(is.null(output.specs$streaming.output.format)) {
    ' '}
  else {
    sprintf(" -outputformat %s", output.specs$streaming.output.format)
  }
  stream.map.input =
    if(input.specs$mode == "binary") {
      " -D stream.map.input=typedbytes"}
    else {''}
  stream.mapred.output =
    if(output.specs$mode == "binary") {
      sprintf(" -D stream.%s.output=typedbytes", if(is.null(reduce)) "map" else "reduce")}
    else {''}
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

  cmds = make.job.conf(otherparams, pfx="-cmdenv")
  
  #debug.opts = "-mapdebug kdfkdfld -reducexdebug jfkdlfkja"
  caches = if(length(cachefiles)>0) make.cache.files(cachefiles, "-files") else " " #<0.21
  archives = if(length(archives)>0) make.cache.files(archives, "-archives") else " "
  mkjars = if(length(jarfiles)>0) make.cache.files(jarfiles, "-libjars", shorten=FALSE) else " "
  
  final.command =
    paste(
      hadoop.command, 
      paste.options(tuning.parameters), 
      stream.map.input, 
      stream.mapred.output, 
      archives, 
      caches, 
      mkjars, 
      input.format, 
          output.format, 
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
                                       function(vr) reduceall(k, vl, vr)))), 
  reduceall  = function(k, vl, vr) keyval(k, list(left = vl, right = vr)))
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
      mapin = mapin[if(mapin[1] == "hdfs:") c(-1, -2) else -1]
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



## push a file through this to get as many partitions as possible (depending on system settings)
## data is unchanged

scatter = function(input, output = NULL)
  mapreduce(input, output, map = function(k, v) keyval(runif(1), keyval(k, v)), 
            reduce = function(k, vv) vv)

##optimizer

is.mapreduce = function(x) {
  is.call(x) && x[[1]] == "mapreduce"}

mapreduce.arg = function(x, arg) {
  match.call(mapreduce, x) [[arg]]}

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



