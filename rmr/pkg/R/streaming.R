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

## loops section, or what runs on the nodes

activate.profiling = function() {
  dir = file.path("/tmp/Rprof", Sys.getenv('mapred_job_id'), Sys.getenv('mapred_tip_id'))
  dir.create(dir, recursive = T)
  Rprof(file.path(dir, paste(Sys.getenv('mapred_task_id'), Sys.time())))}

close.profiling = function() Rprof(NULL)


map.loop = function(map, keyval.reader, keyval.writer, profile) {
  if(profile) activate.profiling()
  kv = keyval.reader()
  while(!is.null(kv)) { 
    out = map(keys(kv), values(kv))
    if(!is.null(out)) {
      keyval.writer(keys(out), values(out))}
    kv = keyval.reader()}
  if(profile) close.profiling()
  invisible()}

list.cmp = function(ll, e) sapply(ll, function(l) isTRUE(all.equal(e, l, check.attributes = FALSE)))
## using isTRUE(all.equal(x)) because identical() was too strict, but on paper it should be it

reduce.loop = function(reduce, keyval.reader, keyval.writer, profile) {
  reduce.flush = function(current.key, vv) {
    out = reduce(current.key, vv)
    if(!is.null(out)) {
      keyval.writer(keys(out), values(out))}}
  if(profile) activate.profiling()
  kv = keyval.reader()
  current.key = keys(kv)
  vv = make.fast.list()
  while(!is.null(kv)) {
    if(identical(keys(kv), current.key)) vv(values(kv)))
    else {
      reduce.flush(current.key, vv())
      current.key = keys(kv)
      vv = make.fast.list(values(kv))}
    kv = keyval.reader()}
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
              keyval.reader = rmr:::make.keyval.reader(input.format$mode, 
  input.format$format), 
  keyval.writer = if(is.null(reduce)) {
  rmr:::make.keyval.writer(output.format$mode, 
  output.format$format)}
  else {
  rmr:::make.keyval.writer()},
  profile = profile.nodes)'
  reduce.line  =  '  rmr:::reduce.loop(reduce = reduce, 
                 keyval.reader = rmr:::make.keyval.reader(), 
  keyval.writer = rmr:::make.keyval.writer(output.format$mode, 
  output.format$format), 
  profile = profile.nodes)'
  combine.line = '  rmr:::reduce.loop(reduce = combine, 
                 keyval.reader = rmr:::make.keyval.reader(), 
  keyval.writer = rmr:::make.keyval.writer(), 
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
    0}}