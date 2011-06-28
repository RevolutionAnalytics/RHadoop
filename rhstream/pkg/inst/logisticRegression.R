## see spark implementation http://www.spark-project.org/examples.html
## see nice derivation here http://people.csail.mit.edu/jrennie/writing/lr.pdf

rhLogisticRegression = function(input, iterations, dims, alpha = -0.0011){
  plane = rnorm(dims)
  g = function(z) 1/(1 + exp(-z))
  for (i in 1:iterations) {
    gradient = rhread(revoMapReduce(input,
      map = function(k, v) keyval (1, g(-v$y * (plane %*% v$x)) * v$y * v$x),
      reduce = function(k, vv) keyval(k, apply(do.call(rbind,vv),2,sum))))
    plane = plane + alpha * gradient[[1]]$val }
  plane }
    
