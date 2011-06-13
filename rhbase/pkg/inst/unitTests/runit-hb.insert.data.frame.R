hb.init(host='127.0.0.1',port=9090)
checkTrue(hb.new.table("dframe","x","y","z"))
N <- 10000
p <- data.frame(x=runif(N),y=sample(1:N,N,replace=TRUE), z=sample(letters,N,replace=TRUE))
checkTrue(hb.insert.data.frame("dframe",p))
hb.delete.table("dframe")

