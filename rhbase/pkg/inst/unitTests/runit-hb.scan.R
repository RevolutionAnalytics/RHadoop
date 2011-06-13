hb.init(host='127.0.0.1',port=9090)
checkTrue(hb.new.table("scan.table","x","y"))
checkTrue(hb.insert("scan.table",list(
                                      list(1,c("x","y"),list(-1,2)),
                                      list(2,c("x","y"),list(-2,3)),
                                      list(3,c("x","y"),list(-3,4)),
                                      list(4,c("x"),list(-4)),
                                      list(5,c("x"),list(-6)),
                                      list(6,c("x"),list(-7)))))
                                      
sc <- hb.scan("scan.table",start=1,colspec=c("x","y"))
checkTrue(all(names(sc)==c("get","close")))

rsx <- c()
while(length(n <- sc$get(1))>0)
  rsx <- c(rsx,n[[1]][[1]])
checkTrue(all(range(rsx)==c(1,6)))

sc <- hb.scan("scan.table",start=1,end=6,colspec=c("x","y"))
checkTrue(all(names(sc)==c("get","close")))
rsx <- c()
while(length(n <- sc$get(1))>0)
  rsx <- c(rsx,n[[1]][[1]])
checkTrue(all(range(rsx)==c(1,5)))


sc <- hb.scan("scan.table",start=1,end=6,colspec=c("x","y"))
checkTrue(all(names(sc)==c("get","close")))
rsx <- c()
while(length(n <- sc$get(1))>0)
  rsx <- c(rsx,n[[1]][[1]])
checkTrue(all(range(rsx)==c(1,5)))


sc <- hb.scan("scan.table",start=1,colspec=c("y"))
checkTrue(all(names(sc)==c("get","close")))
rsx <- c()
while(length(n <- sc$get(1))>0)
  rsx <- c(rsx,n[[1]][[1]])
checkTrue(all(range(rsx)==c(1,3)))


sc <- hb.scan("scan.table",start=NULL,colspec=c("y"))
checkTrue(all(names(sc)==c("get","close")))
rsx <- c()
while(length(n <- sc$get(1))>0)
  rsx <- c(rsx,n[[1]][[1]])
checkTrue(is.null(rsx))

checkTrue(hb.delete.table("scan.table"))
