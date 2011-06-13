## run after hb.new.table
hb.init(host='127.0.0.1',port=9090)
checkTrue(length(hb.list.tables())==1 && names(hb.list.tables)=="testtable")
