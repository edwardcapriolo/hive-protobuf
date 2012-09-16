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
