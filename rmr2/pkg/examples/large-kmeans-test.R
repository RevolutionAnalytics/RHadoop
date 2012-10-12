input.1000 = mapreduce (input = to.dfs(1:1000), 
                        map = function(k, v) keyval(rnorm(1), v), 
                        reduce = to.reduce(identity))

input.10e6 = mapreduce (input = input.1000, 
                        map = function(k, v) lapply(1:1000, function(i) keyval(rnorm(1), v)), 
                        reduce = to.reduce(identity))

kmeans.input.10e6 = mapreduce(input.1000, 
                              map = function(k, v) keyval(rnorm(1), cbind(sample(0:2, recsize, replace = T) + 
                                                                           rnorm(recsize, sd = .1), 
                                                                         sample(0:3, recsize, replace = T) + 
                                                                           rnorm(recsize, sd = .1))))

kmeans.input.10e9 = mapreduce(input.10e6, 
                              map = function(k, v) keyval(rnorm(1), cbind(sample(0:2, recsize, replace = T) + 
                                                                           rnorm(recsize, sd = .1), 
                                                                         sample(0:3, recsize, replace = T) + 
                                                                           rnorm(recsize, sd = .1))))
