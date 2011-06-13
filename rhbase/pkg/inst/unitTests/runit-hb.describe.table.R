## to be run after hb.new.table
hb.init(host='127.0.0.1',port=9090)
checkTrue(is.data.frame(x <- hb.describe.table("testtable")))
checkTrue(nrow(x)==3)
checkTrue(all(rownames(x)==c("x:","y:","z:")))
checkTrue(x["y:","compression"]=="GZ")
