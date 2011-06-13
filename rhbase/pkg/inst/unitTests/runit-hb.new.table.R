## run after hb.init

hb.init(host='127.0.0.1',port=9090)
checkTrue(hb.new.table("testtable", "x","y","z",opts=list(y=list(compression='GZ'))))

