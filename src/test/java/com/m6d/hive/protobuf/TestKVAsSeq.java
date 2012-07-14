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


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;

import com.jointhegrid.hive_test.HiveTestService;
import org.junit.Ignore;

import prototest.Ex.Person;

public class TestKVAsSeq extends HiveTestService {

  public TestKVAsSeq() throws IOException {
    super();
  }

  @Ignore
  public void testRank() throws Exception {
    Path p = new Path(this.ROOT_DIR, "kv");
    SequenceFile.Writer w = SequenceFile.createWriter(this.getFileSystem(), new Configuration(), p, BytesWritable.class, BytesWritable.class);
    Person.Builder bbuild = Person.newBuilder();
    Person ed = bbuild.setEmail("ed@email.com").setName("ed").setId(1).build();

    Assert.assertEquals("ed", ed.getName());

    BytesWritable key = new BytesWritable();
    key.set("ed".getBytes(), 0, 2);
    BytesWritable value = new BytesWritable();
    ByteArrayOutputStream s = new ByteArrayOutputStream();
    ed.writeTo(s);

    Person g = Person.parseFrom(s.toByteArray());
    int bsize = s.toByteArray().length;
    Assert.assertEquals(g.getName(), ed.getName());

    value.set(s.toByteArray(), 0, s.size());

    w.append(key, value);
    w.close();

    //DataOutputBuffer itkey = new DataOutputBuffer();
    //SequenceFile.Reader r = new SequenceFile.Reader(this.getFileSystem(), p, this.createJobConf());
    //SequenceFile.ValueBytes v = r.createValueBytes();

    //while (r.nextRaw(itkey, v) != -1){
    //  v.writeUncompressedBytes(itkey);
    //  Person other = Person.parseFrom(itkey.getData());
    //  Assert.assertEquals(ed.getName(), other.getName());
    //}

    SequenceFile.Reader r = new SequenceFile.Reader(this.getFileSystem(), p, this.createJobConf());
    BytesWritable itKey = new BytesWritable();
    BytesWritable itValue = new BytesWritable();
    r.next(itKey, itValue);
    System.out.println(itValue);

    Assert.assertEquals(bsize, itValue.getLength());
    Assert.assertEquals(bsize, itValue.getSize());
    //Assert.assertEquals(bsize, itValue.getCapacity());
    for (int i = 0; i < bsize; i++) {
      Assert.assertEquals(itValue.getBytes()[i], s.toByteArray()[i]);
    }
    //Assert.assertEquals(itValue.getBytes(), s.toByteArray());
    byte[] whatIWant = new byte[itValue.getLength()];
    System.arraycopy(itValue.getBytes(), 0, whatIWant, 0, itValue.getLength());
    Person other = Person.parseFrom(whatIWant);
    Assert.assertEquals(ed.getName(), other.getName());
  }

  /*
   * test is not cleaning itself. getting lazy
  public void testWithpairs() throws IOException {
    Path p = new Path(this.ROOT_DIR, "kv2");
    Path out = new Path("/tmp/run");
    SequenceFile.Writer w = SequenceFile.createWriter(this.getFileSystem(), new Configuration(), p, BytesWritable.class, BytesWritable.class);

    BytesWritable key = new BytesWritable();
    Person.Builder bbuild1 = Person.newBuilder();
    Person bob = bbuild1.setEmail("bob@email.com").setName("bob").setId(2).build();
    ByteArrayOutputStream s1 = new ByteArrayOutputStream();
    bob.writeTo(s1);
    key.set(s1.toByteArray(), 0, s1.size());

    Person.Builder bbuild = Person.newBuilder();
    Person ed = bbuild.setEmail("ed@email.com").setName("ed").setId(1).build();
    BytesWritable value = new BytesWritable();
    ByteArrayOutputStream s = new ByteArrayOutputStream();
    ed.writeTo(s);
    value.set(s.toByteArray(), 0, s.size());

    w.append(key, value);
    w.close();

    JobConf jc = this.createJobConf();
    jc.setOutputKeyClass(NullWritable.class);
    jc.setOutputValueClass(Pair.class);
    jc.setMapperClass(PrintProtoBufMapper.class);
    jc.setInputFormat(KVAsVSeqFileBinaryInputFormat.class);
    jc.setOutputFormat(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(jc, p);
    FileOutputFormat.setOutputPath(jc, out);
    jc.set("mapred.reduce.tasks", "0");
    JobClient.runJob(jc);


  }
   *
   */
}
