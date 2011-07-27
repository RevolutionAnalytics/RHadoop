## see spark implementation http://www.spark-project.org/examples.html
## see nice derivation here http://people.csail.mit.edu/jrennie/writing/lr.pdf

## create test set as follows
## rhwrite(lapply (1:100, function(i) {eps = rnorm(1, sd =10) ; keyval(i, list(x = c(i,i+eps), y = 2 * (eps > 0) - 1))}), "/tmp/logreg")
## run as:
## rhLogisticRegression("/tmp/logreg", 10, 2, 0.05)
## max likelihood solution diverges for separable dataset, (-inf, inf) such as the above

rhLogisticRegression = function(input, iterations, dims, alpha){
  plane = rep(0, dims)
  g = function(z) 1/(1 + exp(-z))
  for (i in 1:iterations) {
    gradient = rhread(revoMapReduce(input,
      map = function(k, v) keyval (1, v$y * v$x * g(-v$y * (plane %*% v$x))),
      reduce = function(k, vv) keyval(k, apply(do.call(rbind,vv),2,sum)),
      combine = T))
    plane = plane + alpha * gradient[[1]]$val }
  plane }
    
