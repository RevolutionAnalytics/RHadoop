hb.init(host='127.0.0.1',port=9090)
checkTrue(hb.compact.table("testtable",major=FALSE))
checkTrue(hb.compact.table("testtable",major=TRUE))
