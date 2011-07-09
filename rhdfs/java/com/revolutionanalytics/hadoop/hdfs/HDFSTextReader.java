/**
 * Copyright 2009 Saptarshi Guha
 *   
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.revolutionanalytics.hadoop.hdfs;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

public class HDFSTextReader {
    int numlines;
    BufferedReader in;
    FileSystem fs;
    FSDataInputStream filestream;
    String[] linechunk;
    String filename;
    public HDFSTextReader(){}
    public void initialize(FileSystem fs,String filename, int buffersize,int numlines) throws IOException{
	this.filestream = fs.open(new Path(filename),buffersize);
	this.in = new BufferedReader(new InputStreamReader(this.filestream),buffersize);
	this.linechunk = new String[numlines];
	this.numlines = numlines;
	this.filename = filename;
	this.fs=fs;
    }
    public String toString(){
	return("HDFSTextReader[fs="+fs+",filename="+this.filename+", line.chunk="+this.numlines+"]");
    }
    public String[] getLines() throws IOException {
	return(getLines(numlines));
    }
    public String[] getLines(int n) throws IOException {
	if(n != numlines){
	    this.linechunk = new String[n];
	    this.numlines = n;
	}
	for(int i=0;i< this.numlines;i++){
	    String s = in.readLine();
	    if(s==null) {
		String[] r = new String[i];
		for(int j=0;j< i;j++) r[j] = linechunk[j];
		return(r);
	    }else{
		this.linechunk[i] = s;
	    }
	}
	return(this.linechunk);
    }
    public void close() throws IOException{
	in.close();
    }
    
}