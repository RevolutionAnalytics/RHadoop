/**
 * Copyright 2011 Revolution Analytics
 *   
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
