hb.init(host='127.0.0.1',port=9090)
checkTrue(hb.new.table("dframe","x","y","z"))
N <- 10000
p <- data.frame(x=runif(N),y=sample(1:N,N,replace=TRUE), z=sample(letters,N,replace=TRUE))
checkTrue(hb.insert.data.frame("dframe",p))
hb.delete.table("dframe")
getter <- hb.get.data.frame("dframe",start="1",end="10",columns=c("x","y"))
checkTrue({
  x <- getter()
  nrow(x)==9 && all(colnames(x)==c("x","y")) &&  all(x==p[1:9,c("x","y")])
}
          
