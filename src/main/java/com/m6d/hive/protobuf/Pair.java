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
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;


public class Pair implements Writable {

  private Writable key;
  private Writable value;


  public Pair(){

  }

  @Override
  public void write(DataOutput d) throws IOException {
    key.write(d);
    value.write(d);
  }

  @Override
  public void readFields(DataInput di) throws IOException {
    key.readFields(di);
    value.readFields(di);
  }

   public Writable getKey() {
    return key;
  }

  public void setKey(Writable key) {
    this.key = key;
  }

  public Writable getValue() {
    return value;
  }

  public void setValue(Writable value) {
    this.value = value;
  }

}
