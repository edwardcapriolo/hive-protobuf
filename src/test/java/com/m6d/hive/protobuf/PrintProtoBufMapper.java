/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.m6d.hive.protobuf;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author edward
 */
public class PrintProtoBufMapper implements Mapper<NullWritable,Pair,NullWritable,NullWritable>{
 
  @Override
  public void map(NullWritable k1, Pair v1, OutputCollector<NullWritable, NullWritable> oc, Reporter rprtr) throws IOException {
    System.out.println("pair key"+ v1.getKey());
    System.out.println("pair value"+ v1.getValue());
  }

  @Override
  public void configure(JobConf jc) {

  }

  @Override
  public void close() throws IOException {

  }


}
