/*
Copyright 2012 m6d.com

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.m6d.hive.protobuf;

import java.io.DataInput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

/* Hive ignores keys in input formats which is PITA.
 * This input format converts:
 * key,value -> ull,pair(key,value)
 */
public class KVAsVSeqFileBinaryInputFormat 
        extends SequenceFileInputFormat<NullWritable,Pair> {

  public KVAsVSeqFileBinaryInputFormat() {
    super();
  }

  public RecordReader<NullWritable,Pair> getRecordReader
          (InputSplit split, JobConf jobConf, Reporter reporter)
  throws IOException {
    return new KVAsVSeqFileBinaryRecordReader(jobConf, (FileSplit) split);
  }

  public static class KVAsVSeqFileBinaryRecordReader
          implements RecordReader<NullWritable,Pair> {

    private SequenceFile.Reader in;
    private long start;
    private long end;
    private boolean done = false;
    private DataOutputBuffer buffer = new DataOutputBuffer();
    private SequenceFile.ValueBytes vbytes;

    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();

    public KVAsVSeqFileBinaryRecordReader(Configuration conf, FileSplit split)
    throws IOException {
      Path path = split.getPath();
      FileSystem fs = path.getFileSystem(conf);
      this.in = new SequenceFile.Reader(fs, path, conf);
      this.end = split.getStart() + split.getLength();
      if (split.getStart() > in.getPosition()){
        in.sync(split.getStart());
      }
      this.start = in.getPosition();
      vbytes = in.createValueBytes();
      done = start >= end;
    }

    @Override
    public boolean next(NullWritable k, Pair v) throws IOException {
      done=in.next(key, value);
      v.setKey(key);
      v.setValue(value);
      return done;
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    public String getKeyClassName(){
      return NullWritable.class.getName();
    }

    public String getValueClassName(){
      return Pair.class.getName();
    }

    @Override
    public Pair createValue() {
      return new Pair();
    }

    @Override
    public long getPos() throws IOException {
      return in.getPosition();
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public float getProgress() throws IOException {
      if (end == start){
        return 0.0f;
      } else {
        return Math.min(1.0f, (float) ((in.getPosition() - start) /
                (double) (end-start)));
      }
    }

  }

}