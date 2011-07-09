package com.revolutionanalytics.hadoop.hdfs;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileSystem;

public abstract class DelayedExceptionThrowing {
    abstract void process(Path p, FileSystem srcFs) throws IOException;
	
    final void globAndProcess(Path srcPattern, FileSystem srcFs
			      ) throws IOException {
	ArrayList<IOException> exceptions = new ArrayList<IOException>();
	for(Path p : FileUtil.stat2Paths(srcFs.globStatus(srcPattern), 
					 srcPattern))
	    try { process(p, srcFs); } 
	    catch(IOException ioe) { exceptions.add(ioe); }
    
	if (!exceptions.isEmpty())
	    if (exceptions.size() == 1)
		throw exceptions.get(0);
	    else 
		throw new IOException("Multiple IOExceptions: " + exceptions);
    }
}