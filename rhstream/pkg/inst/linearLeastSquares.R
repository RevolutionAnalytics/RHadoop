##input is X: k = (i,j) , v = x_{ij}
##

swap = function(x) list(x[[2]], x[[1]])

transposeMap = mkMap(swap, identity)

# test matrix
# X = do.call(c, lapply(1:2, function(i) lapply(1:2, function(j) keyval(c(i,j), rnorm(1)))))
rhTranspose = function(input, output = NULL)
  warning("This examples is neither finished nor tested, please do not run")
  revoMapReduce(input = input, output = output, map = transposeMap)

matMulMap = function(i) function(k,v) keyval(k[[i]], list(pos = k, elem = v))

rhMatMult = function(left, right, result = NULL) {
  warning("This examples is neither finished nor tested, please do not run")
  revoMapReduce(
                input =
                rhRelationalJoin(leftinput = left, rightinput = right,
                                 map.left.keyval = matMulMap(2),
                                 map.right.keyval = matMulMap(1),
                                 reduce.keyval = function(k, vl, vr) keyval(c(vl$pos[[1]], vr$pos[[2]]), vl$elem*vr$elem)),
                output = result,
                map = mkMap(identity),
                reduce = mkReduce(identity, function(x) sum(unlist(x))))}

rhLinearLeastSquares = function(X,y) {
  warning("This examples is neither finished nor tested, please do not run")
  XtX = rhread(rhMatMult(rhTranspose(X), X))
  Xty = rhread(rhMatMul(rhTranspose(X), y))
  solve(XtX,Xty)}
