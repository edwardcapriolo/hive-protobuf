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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.junit.Assert;

import com.jointhegrid.hive_test.HiveTestService;

import prototest.Ex;
import prototest.Ex.Hobby;
import prototest.Ex.Person;

public class LongTest extends HiveTestService {

  static java.util.Random r ;
  
  static int load =4000;
  
  static {
     r = new java.util.Random(10);
  }
  public LongTest() throws IOException {
    super();
  }

  public static int randomInt(){
    return r.nextInt();
  }

  public static String randomString() {
    char[] word = new char[r.nextInt(8) + 3]; // words of length 3 through 10. (1 and 2 letter words are boring.)
    for (int j = 0; j < word.length; j++) {
      word[j] = (char) ('a' + r.nextInt(26));
    }
    return new String(word);

  }


   public void testWriteReadProto() throws Exception {
    Path p = new Path(this.ROOT_DIR, "reallybigfile2");


    SequenceFile.Writer w = SequenceFile.createWriter(this.getFileSystem(),
            new Configuration(), p, BytesWritable.class, BytesWritable.class,
            SequenceFile.CompressionType.BLOCK);

    long startLoad = System.currentTimeMillis();
    int toLoad = load;
     for (int i = 0; i < toLoad; i++) {
       Person.Builder bbuild = Person.newBuilder();
       Person ed = bbuild.setEmail(randomString()).setName(randomString()).
               setId(randomInt()).setHobby(Hobby.newBuilder().setName(randomString())).build();
       Person bo = bbuild.setEmail(randomString()).setName(randomString()).
               setId(randomInt()).setHobby(Hobby.newBuilder().setName(randomString())).build();

      BytesWritable key = new BytesWritable();
      BytesWritable value = new BytesWritable();
      ByteArrayOutputStream s = new ByteArrayOutputStream();
      ed.writeTo(s);


      ByteArrayOutputStream t = new ByteArrayOutputStream();
      bo.writeTo(t);

      key.set(s.toByteArray(), 0, s.size());
      value.set(t.toByteArray(), 0, t.size());
      w.append(key, value);
    }
    w.close();

    long start = System.currentTimeMillis();
    SequenceFile.Reader r = new SequenceFile.Reader(this.getFileSystem(), p, this.createJobConf());
    BytesWritable readkey = new BytesWritable();
    BytesWritable readval = new BytesWritable();
    while (r.next(readkey, readval)){
     byte [] c = new byte [ readkey.getLength()];
      System.arraycopy(readkey.getBytes(), 0, c, 0, readkey.getLength());
      Person.parseFrom(c);

           byte [] d = new byte [ readval.getLength()];
      System.arraycopy(readval.getBytes(), 0, d, 0, readval.getLength());
      Person.parseFrom(d);
    }
    long end = System.currentTimeMillis();

    System.out.println("reading proto took"+ (end-start));
    r.close();
  }
  public void testBigProto() throws Exception {
    Path p = new Path(this.ROOT_DIR, "reallybigfile");


    SequenceFile.Writer w = SequenceFile.createWriter(this.getFileSystem(),
            new Configuration(), p, BytesWritable.class, BytesWritable.class,
            SequenceFile.CompressionType.BLOCK);

    long startLoad = System.currentTimeMillis();
    int toLoad = load;
    for (int i = 0; i < toLoad; i++) {
      Person.Builder bbuild = Person.newBuilder();
    //  Person ed = bbuild.setEmail("ed@email.com").setName("ed").
    //          setId(i).setHobby(Hobby.newBuilder().setName("java")).build();

       Person ed = bbuild.setEmail(randomString()).setName(randomString()).
              setId(randomInt()).setHobby(Hobby.newBuilder().setName(randomString())).build();


     // Person bo = bbuild.setEmail("bo@email.com").setName("bo").
      //        setId(i).setHobby(Hobby.newBuilder().setName("bball")).build();
        Person bo = bbuild.setEmail(randomString()).setName(randomString()).
              setId(randomInt()).setHobby(Hobby.newBuilder().setName(randomString())).build();

      BytesWritable key = new BytesWritable();
      BytesWritable value = new BytesWritable();
      ByteArrayOutputStream s = new ByteArrayOutputStream();
      ed.writeTo(s);


      ByteArrayOutputStream t = new ByteArrayOutputStream();
      bo.writeTo(t);

      key.set(s.toByteArray(), 0, s.size());
      value.set(t.toByteArray(), 0, t.size());
      w.append(key, value);
    }
    w.close();
    System.out.println("len " + this.getFileSystem().getFileStatus(p).getLen());
    long endLoad = System.currentTimeMillis();
    System.out.println((endLoad - startLoad) + " time taken loading");

    String jarFile;
    jarFile = KVAsVSeqFileBinaryInputFormat.class.getProtectionDomain().getCodeSource().getLocation().getFile();
    client.execute("add jar " + jarFile);
    client.execute("set hive.aux.jars.path=file:///" + jarFile);

    client.execute("create table  bigproto   "
            + " ROW FORMAT SERDE '" + ProtobufDeserializer.class.getName() + "'"
            + " WITH SERDEPROPERTIES ('KEY_SERIALIZE_CLASS'='" + Ex.Person.class.getName()
            + "','VALUE_SERIALIZE_CLASS'='" + Ex.Person.class.getName() + "'   )"
            + " STORED AS INPUTFORMAT '" + KVAsVSeqFileBinaryInputFormat.class.getName() + "'"
            + " OUTPUTFORMAT '" + SequenceFileOutputFormat.class.getName() + "'");

    client.execute("load data local inpath '" + p.toString() + "' into table bigproto");

    long startQuery = System.currentTimeMillis();
    client.execute("SELECT count(1) FROM bigproto");
    List<String> results = client.fetchAll();
    Assert.assertEquals(toLoad + "", results.get(0));
    long endQuery = System.currentTimeMillis();

    System.out.println((endQuery - startQuery) + " Proto Query time taken");
    client.execute("drop table bigproto");

  }

  public void testBigDat() throws Exception {
    Path p = new Path(this.ROOT_DIR, "reallybigflat");
    SequenceFile.Writer w = SequenceFile.createWriter(this.getFileSystem(),
            new Configuration(), p, NullWritable.class, Text.class, SequenceFile.CompressionType.BLOCK);

    long startLoad = System.currentTimeMillis();
    int toLoad = load;
    for (int i = 0; i < toLoad; i++) {
      Text t = new Text();
      //t.set("ed\ted@email.com\t1\tjava\tbob\tbob@email.com\t3\tbball");
      t.set(randomString()+"\t"+randomString()+"\t"+randomInt()+"\t"+randomString()+"\t"+
        randomString()+"\t"+randomString()+"\t"+randomInt()+"\t"+randomString());

      w.append(NullWritable.get(), t);
    }
    w.close();
    System.out.println("len " + this.getFileSystem().getFileStatus(p).getLen());
    long endLoad = System.currentTimeMillis();
    System.out.println((endLoad - startLoad) + " time taken loading");

    String jarFile;
    jarFile = KVAsVSeqFileBinaryInputFormat.class.getProtectionDomain().getCodeSource().getLocation().getFile();
    client.execute("add jar " + jarFile);
    client.execute("set hive.aux.jars.path=file:///" + jarFile);

    client.execute("create table  bigtext   "
            + "(name string, email string , id int , hobby string, "
            + " name1 string, email1 string, id1 int , hobby1 string)"
            + " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS SEQUENCEFILE");

    client.execute("load data local inpath '" + p.toString() + "' into table bigtext");

    long startQuery = System.currentTimeMillis();
    //client.execute( "select distinct(name) from bigtext");
    //List<String> result = client.fetchAll();
    //Assert.assertEquals("edward", result);
    client.execute("SELECT count(1) FROM bigtext");
    List<String> results = client.fetchAll();
    Assert.assertEquals(toLoad + "", results.get(0));
    long endQuery = System.currentTimeMillis();

    System.out.println((endQuery - startQuery) + " Query time taken");
    client.execute("drop table bigproto");

  }
}
