package com.m6d.hive.protobuf;

import com.m6d.hive.protobuf.Pair;
import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import prototest.Ex;

public class PairMapper implements Mapper<NullWritable,Pair,Text,Text>  {

  @Override
  public void map(NullWritable k1, Pair v1, OutputCollector<Text, Text> oc,
          Reporter rprtr) throws IOException {
     BytesWritable key_bytes = (BytesWritable) v1.getKey();
     byte [] b = new byte [key_bytes.getLength()];
     System.arraycopy(key_bytes.getBytes(), 0, b, 0, key_bytes.getLength());

     BytesWritable value_bytes = (BytesWritable) v1.getValue();
     byte [] c = new byte [value_bytes.getLength()];
     System.arraycopy(value_bytes.getBytes(), 0, c, 0, value_bytes.getLength());

     //Now here you can reconstruct your protobufs!
     //Ex.Car myCar = Ex.Car.parseFrom( b );
     //System.out.println(myCar.getAccessoriesCount());
     //System.out.println(myCar.getAccessoriesList().get(0).getName());
  }

  @Override
  public void configure(JobConf jc) {
  }

  @Override
  public void close() throws IOException {
  }

}
