/**
 * Copyright 2010 Revolution Analytics
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

package com.revolutionanalytics.hadoop.rhstr;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LApplyInputFormat implements InputFormat<Text, Text>, JobConfigurable
{
    public static final Log LOG = LogFactory.getLog("com.revolutionanalytics.hadoop.rhstr");
    protected static class LApplyReader implements RecordReader<Text, Text> {
	private JobConf job;
	private LApplyInputSplit split;
	private long leftover;
	private long pos = 0; 
	protected LApplyReader(LApplyInputSplit split, JobConf job) throws IOException{
	    this.split = split;
	    this.leftover = split.getLength();
	}
	public void close()  throws IOException{ }
	public Text createKey() {
	    return new Text();	
	}
	public Text createValue() {
	    return new Text();
	}
	public long getPos() throws IOException {
	    return pos;
	}
	public float getProgress() throws IOException {
	    return pos / (float) split.getLength();
	}
	public boolean next(Text key, Text value) throws IOException {
	    if (leftover == 0) return false;
	    long wi = pos + split.getStart();
	    key.set("");
	    value.set(String.valueOf(wi+1));
	    pos ++; leftover --;
	    return true;
	}
    }
	
    public LApplyInputFormat() {}

    public void validateInput(JobConf job) {}

    public void configure(JobConf job){

    }
	
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
	long n = job.getInt("rhstream_numiterations",0)*1L;
	long chunkSize = n / (numSplits == 0 ? 1 : numSplits);
	LOG.info("Iterations="+n+" NumSplits="+numSplits);	
	InputSplit[] splits = new InputSplit[numSplits];
	for (int i = 0; i < numSplits; i++) {
	    LApplyInputSplit split;
	    if ((i + 1) == numSplits)
		split = new LApplyInputSplit(i * chunkSize, n);
	    else
		split = new LApplyInputSplit(i * chunkSize, (i * chunkSize) + chunkSize);
	    splits[i] = split;
	}
	return splits;
    }

    public RecordReader<Text, Text> getRecordReader(InputSplit split,JobConf job, Reporter reporter) throws IOException {
	return new LApplyReader((LApplyInputSplit) split, job);
    }


    protected static class LApplyInputSplit implements InputSplit {
	private long end = 0;
	private long start = 0;
	public LApplyInputSplit() {
	}
	/**
	 * Convenience Constructor
	 * @param start the index of the first row to select
	 * @param end the index of the last row to select (non-inclusive)
	 */
	public LApplyInputSplit(long start, long end) {
	    this.start = start;
	    this.end = end-1;
	}

	/**
	 * @return The index of the first row to select
	 */
	public long getStart() {
	    return start;
	}
			
	/**
	 * @return The index of the last row to select
	 */
	public long getEnd() {
	    return end;
	}
	/**
	 * @return The total row count in this split
	 */
	public long getLength() throws IOException {
	    return end - start + 1;
	}

	public String[] getLocations() {
	    return new String[] { };
	}

	/** {@inheritDoc} */
	public void readFields(DataInput input) throws IOException {
	    start = input.readLong();
	    end = input.readLong();
	}
			
	/** {@inheritDoc} */
	public void write(DataOutput output) throws IOException {
	    output.writeLong(start);
	    output.writeLong(end);
	}

    }
	
}

