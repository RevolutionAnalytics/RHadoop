#create new table with columns x,y and z
hb.new.table("mytable", "x","y","z",opts=list(y=list(compression='GZ')))
 
#create a string serializer for data to hbase
str_sz<-function(x)(charToRaw(toString(x)))
 
#insert some values into the table
hb.insert("mytable",list( list(1,c("x","y","z"),list("apple","berry","cherry"))),sz=str_sz)
hb.insert("mytable",list( list(2,c("x","y","z"),list(10001,14,575))),sz=str_sz)
 
#create a deserializer that returns everything as a string
str_usz<-function(x)(rawToChar(x))
 
read the contents of the table
hb.get("mytable",list(1,2),c("x","y","z"), sz=str_sz, usz=str_usz)