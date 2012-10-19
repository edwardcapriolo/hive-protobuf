package com.m6d.hive.protobuf;

import com.jointhegrid.hive_test.HiveTestService;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.junit.Test;
import prototest.Ex.Accessory;
import prototest.Ex.Car;

public class TestMRonProtobuf extends HiveTestService {

  public TestMRonProtobuf()
  throws IOException{
    //super(LOCAL_MR,LOCAL_FS,1,1);
    super();
  }

  @Test
  public void testIt() throws Exception {
     String table="emtpymrcar";
        Path p = new Path(this.ROOT_DIR, table);
    SequenceFile.Writer w = SequenceFile.createWriter(this.getFileSystem(),
            new Configuration(), p, BytesWritable.class, BytesWritable.class);

    Car.Builder car = Car.newBuilder();
    Accessory.Builder acc = Accessory.newBuilder();
    acc.setName("ac");
    acc.setValue("equipped");
    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();
    ByteArrayOutputStream s = new ByteArrayOutputStream();

    car.addAccessories( acc );
    car.build().writeTo(s);

    ByteArrayOutputStream t = new ByteArrayOutputStream();

    key.set(s.toByteArray(), 0, s.size());
    value.set(t.toByteArray(), 0, t.size());
    w.append(key, value);
    w.close();

    JobConf jc = this.createJobConf();
    jc.setInputFormat(KVAsVSeqFileBinaryInputFormat.class);
    jc.setMapOutputKeyClass(NullWritable.class);
    jc.setMapOutputValueClass(Pair.class);
    jc.setMapperClass(PairMapper.class);
    FileInputFormat.setInputPaths(jc, p);
    jc.setOutputFormat(NullOutputFormat.class);

    JobClient client = new JobClient(jc);
    client.runJob(jc);

  }

}
