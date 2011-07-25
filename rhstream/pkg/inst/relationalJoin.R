rhRelationalJoin = function(
  leftinput = NULL,
  rightinput = NULL,
  input = NULL,
  output = NULL,
  leftouter = F,
  rightouter = F,
  fullouter = F,
  map.left.keyval = mkMap(identity),
  map.right.keyval = mkMap(identity),
  reduce.keyval  = function (k, vl, vr) keyval(k, list(left=vl, right=vr)))
{
  stopifnot(xor(!is.null(leftinput), !is.null(input) &&
                (is.null(leftinput)==is.null(rightinput))))
  stopifnot(xor(xor(leftouter, rightouter), fullouter) ||
            !(leftouter || rightouter || fullouter))
  if (is.null(leftinput)) {
    leftinput = input}
  markSide =
    function(kv, isleft) keyval(kv$key, list(val = kv$val, isleft = isleft))
  isLeftSide = 
    function(leftinput) {
      paste("file", sub("//", "/", leftinput), sep = ":") == Sys.getenv("map_input_file")}
  reduce.split =
    function(vv) tapply(lapply(vv, function(v) v$val), sapply(vv, function(v) v$isleft), identity, simplify = FALSE)
  padSide =
    function(vv, sideouter, fullouter) if (length(vv) == 0 && (sideouter || fullouter)) c(NA) else vv
  map = if (is.null(input)) {
    function(k,v) {
      ils = isLeftSide(leftinput)
      markSide(if(ils) map.left.keyval(k,v) else map.right.keyval(k,v), ils)
    }
  }
  else {
    function(k,v) {
      list(markSide(map.left.keyval(k,v), TRUE),
           markSide(map.right.keyval(k,v), FALSE))
    }
  }
  revoMapReduce(map = map,
                reduce =
                function(k, vv) {
                  save(vv, file = paste("/tmp/reduce_vv", k, sep = "_"))
                  rs = reduce.split(vv)
                  save(rs, file = paste("/tmp/reduce_rs", k, sep = "_"))
                  values.left = padSide(rs$`TRUE`, rightouter, fullouter)
                  values.right = padSide(rs$`FALSE`, leftouter, fullouter)
                  do.call(c,
                          lapply(values.left,
                                 function(x) lapply(values.right,
                                                    function(y) reduce.keyval(k, x, y))))},
                ## combiner = F,
                input = c(leftinput,rightinput),
                output = output)
}

