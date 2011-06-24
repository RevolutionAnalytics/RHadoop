relationalJoin = function(
  leftinput = NULL,
  rightinput = NULL,
  input = NULL,
  output,
  leftouter = F,
  rightouter = F,
  fullouter = F,
  map.left.keyval = mkMapper(identity),
  map.right.keyval = mkMapper(identity),
  reduce.keyval  = function (k, vl, vr) keyval(k, list(left=vl, right=vr))) {
stopifnot(leftouter && rightouter == fullouter)
  if (is.null(leftinput)) {
    leftinput = input}
  markSide = function(kv, isleft) keyval(kv$key, list(val = kv$value, isleft =isleft))
  isLeftSide = function(leftinput) length(grep( paste("^file:", leftinput, sep = ""),  Sys.getenv("mapred.input.file")))
  reduce.split = function(vv) tapply(lapply(vv, function(v) v$value), lapply(vv, function(v) v$isleft))
  padSide = function(vv, sideouter, fullouter) if (length(vv) == 0 && (sideouter || fullouter)) c(NA) else vv
  RevoMapReduce(map =
                if (is.null(input)) {
                  function(k,v) {
                    ils = isLeftSide(leftinput)
                    markSide(if(ils) map.left.keyval(k,v) else map.right.keyval(k,v), ils)}}
                else {
                  function(k,v) {
                    list(markSide(map.left.keyval(k,v), TRUE),
                         markSide(map.right.keyval(k,v), FALSE))
                  }
                },
                reduce =
                function(k, vv) {
                  rs = reduce.split(vv)
                  values.left = padSide(rs$`TRUE`)
                  value.right = padSide(rs$`FALSE`)
                  if(length(values.left) == 0 && (rightouter || fullouter)) {
                    values.left = c(NA)}
                  if(length(values.right) == 0 && (leftouter || fullouter)) {
                    values.right = c(NA)}
                  lapply(values.left,
                         function(x) lapply(values.right,
                                            function(y) reduce.keyval(k, x, y)))},
                ## combiner = F,
                input = c(leftinput,rightinput),
                output = output)
}

