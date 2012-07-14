/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
