package com.m6d.hive.protobuf;
import com.jointhegrid.hive_test.HiveTestService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.junit.Assert;
import prototest.Ex;
import prototest.Ex.AList;
import prototest.Ex.AThing;
import prototest.Ex.AddressBook;
import prototest.Ex.Car;
import prototest.Ex.Hobby;
import prototest.Ex.Person;
import prototest.Ex.Tire;
import prototest.Ex.TireMaker;


public class TestProtoCar extends HiveTestService{

  public TestProtoCar() throws IOException {
    super();
  }

  public void testemptyCar() throws Exception {
    String table="emtpycar";
        Path p = new Path(this.ROOT_DIR, table);
    SequenceFile.Writer w = SequenceFile.createWriter(this.getFileSystem(),
            new Configuration(), p, BytesWritable.class, BytesWritable.class);

    Car.Builder car = Car.newBuilder();

    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();
    ByteArrayOutputStream s = new ByteArrayOutputStream();
    car.build().writeTo(s);

    ByteArrayOutputStream t = new ByteArrayOutputStream();
    
    key.set(s.toByteArray(), 0, s.size());
    value.set(t.toByteArray(), 0, t.size());
    w.append(key, value);
    w.close();

    String jarFile;
    jarFile = KVAsVSeqFileBinaryInputFormat.class.getProtectionDomain().getCodeSource().getLocation().getFile();

    client.execute("add jar " + jarFile);
    client.execute("set hive.aux.jars.path=file:///"+jarFile);

    client.execute("create table     "+table+" "
            + " ROW FORMAT SERDE '" + ProtobufDeserializer.class.getName() + "'"
            + " WITH SERDEPROPERTIES ('KEY_SERIALIZE_CLASS'='" + Ex.Car.class.getName()
            //+ " WITH SERDEPROPERTIES ('KEY_SERIALIZE_CLASS'='" + Ex.AList.class.getName()
            //+ "','VALUE_SERIALIZE_CLASS'='" + Ex.AList.class.getName()
            + "'   )"
            + " STORED AS INPUTFORMAT '" + KVAsVSeqFileBinaryInputFormat.class.getName() + "'"
            + " OUTPUTFORMAT '" + SequenceFileOutputFormat.class.getName() + "'");

    client.execute("load data local inpath '" + p.toString() + "' into table "+table+" ");
    client.execute("SELECT key FROM "+table);

    List<String> results = client.fetchAll();
    String expected="{\"accessoriescount\":0,\"accessorieslist\":[],\"tirescount\":0,\"tireslist\":[]}";
    //expected = "{\"agecount\":2,\"agelist\":[2,3],\"thingscount\":1,\"thingslist\":[{\"luckynumberscount\":2,\"luckynumberslist\":[7,4],\"toyscount\":1,\"toyslist\":[\"car\"]}]}";
    Assert.assertEquals(expected, results.get(0));
    client.execute("drop table "+table);

  }


  public void testWithTire() throws Exception {
    String table="nulltire";
        Path p = new Path(this.ROOT_DIR, table);
    SequenceFile.Writer w = SequenceFile.createWriter(this.getFileSystem(),
            new Configuration(), p, BytesWritable.class, BytesWritable.class);

    Car.Builder car = Car.newBuilder();
    Tire.Builder tire = Tire.newBuilder();
    car.addTires(tire.build());

    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();
    ByteArrayOutputStream s = new ByteArrayOutputStream();
    car.build().writeTo(s);

    ByteArrayOutputStream t = new ByteArrayOutputStream();

    key.set(s.toByteArray(), 0, s.size());
    value.set(t.toByteArray(), 0, t.size());
    w.append(key, value);
    w.close();

    String jarFile;
    jarFile = KVAsVSeqFileBinaryInputFormat.class.getProtectionDomain().getCodeSource().getLocation().getFile();

    client.execute("add jar " + jarFile);
    client.execute("set hive.aux.jars.path=file:///"+jarFile);

    client.execute("create table     "+table+" "
            + " ROW FORMAT SERDE '" + ProtobufDeserializer.class.getName() + "'"
            + " WITH SERDEPROPERTIES ('KEY_SERIALIZE_CLASS'='" + Ex.Car.class.getName()
            //+ " WITH SERDEPROPERTIES ('KEY_SERIALIZE_CLASS'='" + Ex.AList.class.getName()
            //+ "','VALUE_SERIALIZE_CLASS'='" + Ex.AList.class.getName()
            + "'   )"
            + " STORED AS INPUTFORMAT '" + KVAsVSeqFileBinaryInputFormat.class.getName() + "'"
            + " OUTPUTFORMAT '" + SequenceFileOutputFormat.class.getName() + "'");

    client.execute("load data local inpath '" + p.toString() + "' into table "+table+" ");
    client.execute("SELECT key FROM "+table);

    List<String> results = client.fetchAll();
    String expected="{\"accessoriescount\":0,\"accessorieslist\":[],\"tirescount\":1,\"tireslist\":[{\"tiremaker\":null,\"tirepressure\":null}]}";
    Assert.assertEquals(expected, results.get(0));
    client.execute("drop table "+table);

  }


   public void testWithTireMaker() throws Exception {
    String table="nulltire";
        Path p = new Path(this.ROOT_DIR, table);
    SequenceFile.Writer w = SequenceFile.createWriter(this.getFileSystem(),
            new Configuration(), p, BytesWritable.class, BytesWritable.class);
    TireMaker.Builder maker = TireMaker.newBuilder();
    maker.setMaker("badyear");
    Car.Builder car = Car.newBuilder();
    Tire.Builder tire = Tire.newBuilder();
    car.addTires(tire.setTireMaker(maker.build()).build());

    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();
    ByteArrayOutputStream s = new ByteArrayOutputStream();
    car.build().writeTo(s);

    ByteArrayOutputStream t = new ByteArrayOutputStream();

    key.set(s.toByteArray(), 0, s.size());
    value.set(t.toByteArray(), 0, t.size());
    w.append(key, value);
    w.close();

    String jarFile;
    jarFile = KVAsVSeqFileBinaryInputFormat.class.getProtectionDomain().getCodeSource().getLocation().getFile();

    client.execute("add jar " + jarFile);
    client.execute("set hive.aux.jars.path=file:///"+jarFile);
    client.execute("create table     "+table+" "
            + " ROW FORMAT SERDE '" + ProtobufDeserializer.class.getName() + "'"
            + " WITH SERDEPROPERTIES ('KEY_SERIALIZE_CLASS'='" + Ex.Car.class.getName()
            + "'   )"
            + " STORED AS INPUTFORMAT '" + KVAsVSeqFileBinaryInputFormat.class.getName() + "'"
            + " OUTPUTFORMAT '" + SequenceFileOutputFormat.class.getName() + "'");
    client.execute("load data local inpath '" + p.toString() + "' into table "+table+" ");
    client.execute("SELECT key FROM "+table);

    List<String> results = client.fetchAll();
    String expected="{\"accessoriescount\":0,\"accessorieslist\":[],\"tirescount\":1,\"tireslist\":[{\"tiremaker\":{\"maker\":\"badyear\",\"price\":null},\"tirepressure\":null}]}";
    Assert.assertEquals(expected, results.get(0));
    client.execute("drop table "+table);

  }




}