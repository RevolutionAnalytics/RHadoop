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
import java.io.File;
import java.io.Writer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileSystem;

public class FileUtils {
    private static final SimpleDateFormat formatter =  new SimpleDateFormat("yyyy-MM-dd HH:mm");
    static final String COPYTOLOCAL_PREFIX = "_copyToLocal_";
    private static final String fsep="\t";

    public FileUtils(Configuration cfg){
    }

    public static void makeFolder(FileSystem fs,String[] s) throws IOException{
	for(int i=0;i<s.length;i++){
	    Path p = new Path(s[i]);
	    fs.mkdirs(p);
	}
    }
    public static void setPermissions(FileSystem fs,String[] p, String[] s) throws IOException{
	for(int i=0;i<p.length;i++){
	    fs.setPermission(new Path(p[i]), new org.apache.hadoop.fs.permission.FsPermission(s[i]));
	}
    }
    public static void setOwner(FileSystem fs,String[] p, String[] ow,String[] gp) throws IOException{
	for(int i=0;i<p.length;i++){
	    String a,b;
	    if(ow[i].equals("")) a=null;else a=ow[i];
	    if(gp[i].equals("")) b=null;else b=gp[i];
	    if(a==null && b==null) continue;
	    fs.setOwner(new Path(p[i]), a,b);
	}
    }

    public static Path[] makePathsFromStrings(String[] f){
	Path[] p = new Path[f.length];
	for(int i=0;i< f.length;i++) p[i] = new Path(f[i]);
	return(p);
    }

    public static String[] ls(FileSystem srcFS,String[] r,int f) throws IOException,FileNotFoundException{
	ArrayList<String> lsco = new ArrayList<String>();
	for(int i=0;i<r.length;i++){
	    String path = r[i];
	    ls__(srcFS,path,lsco,f>0 ? true:false);
	}
	return(lsco.toArray(new String[]{}));
    }

    private static void ls__(FileSystem srcFS,String path, ArrayList<String> lsco,boolean dorecurse)  
	throws IOException,FileNotFoundException{
	Path spath = new Path(path);
	FileStatus[] srcs;
	srcs = srcFS.globStatus(spath);
	if (srcs==null || srcs.length==0) {
	    throw new FileNotFoundException("Cannot access " + path + 
					    ": No such file or directory.");
	}
	if(srcs.length==1 && srcs[0].isDir())
	    srcs = srcFS.listStatus(srcs[0].getPath());
	Calendar c =  Calendar.getInstance();
	for(FileStatus status : srcs){
	    StringBuilder sb = new StringBuilder();
	    boolean idir = status.isDir();
	    String x= idir? "d":"-";
	    if(dorecurse && idir) 
		ls__(srcFS,status.getPath().toUri().getPath(),lsco,dorecurse);
	    else{
		sb.append(x);
		sb.append(status.getPermission().toString());
		sb.append(fsep);
		
		sb.append(status.getOwner());
		sb.append(fsep);
		
		sb.append(status.getGroup());
		sb.append(fsep);
		
		sb.append(status.getLen());
		sb.append(fsep);
		
		Date d = new Date(status.getModificationTime());
		sb.append(formatter.format(d));
		sb.append(fsep);
		
		sb.append(status.getPath().toUri().getPath());
		lsco.add(sb.toString());
	    }
	}
    }


    public static void delete(final Configuration cfg,FileSystem srcFS,String[] srcf, final boolean recursive) throws IOException {
	for(int i=0;i < srcf.length;i++){
	    String sc=srcf[i];
	    Path srcPattern = new Path(sc);
	    new DelayedExceptionThrowing() {
		void process(Path p, FileSystem srcFS) throws IOException {
		    delete(cfg,srcFS,p,  recursive);
		}
	    }.globAndProcess(srcPattern, srcFS);
	}
    }
    
    private static void delete(Configuration cfg,FileSystem srcFS,Path src, boolean recursive) 
	throws IOException {
	Trash trashTmp = new Trash(srcFS, cfg);
	if (trashTmp.moveToTrash(src)) {
	    System.out.println("Moved to trash: " + src);
	    return;
	}
	if (srcFS.delete(src, true)) {
	    System.out.println("Deleted " + src);
	} else {
	    if (!srcFS.exists(src)) {
		throw new FileNotFoundException("cannot remove "
						+ src + ": No such file or directory.");
	    }
	    throw new IOException("Delete failed " + src);
	}
    }
    
}
