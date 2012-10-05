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
import prototest.Ex.Hobby;
import prototest.Ex.Person;

public class TestProto extends HiveTestService {

  public TestProto() throws IOException {
    super();
  }

  public void testArrayParse() {
    AList.Builder abuild = AList.newBuilder();

    ProtobufDeserializer de = new ProtobufDeserializer();
    List<String> cols = new ArrayList<String>();
    List<TypeInfo> types = new ArrayList<TypeInfo>();
    de.populateTypeInfoForClass(abuild.build().getClass(), cols, types, 0);

    ArrayList<String> alistExpectedCols = new ArrayList();
    alistExpectedCols.add("AgeCount");
    alistExpectedCols.add("AgeList");
    alistExpectedCols.add("ThingsCount");
    alistExpectedCols.add("ThingsList");
    Assert.assertEquals(alistExpectedCols, cols);

    ArrayList<TypeInfo> alistExpectedTypes = new ArrayList<TypeInfo>();
    alistExpectedTypes.add(TypeInfoFactory.intTypeInfo);
    alistExpectedTypes.add(TypeInfoFactory.getListTypeInfo(TypeInfoFactory.intTypeInfo));
    alistExpectedTypes.add(TypeInfoFactory.intTypeInfo);

    List<String> aThingCols = Arrays.<String>asList( "LuckynumbersCount",
            "LuckynumbersList", "ToysCount", "ToysList");
    List<TypeInfo> aThingTypes = new ArrayList<TypeInfo>();
    aThingTypes.add(TypeInfoFactory.intTypeInfo);
    aThingTypes.add(TypeInfoFactory.getListTypeInfo(TypeInfoFactory.intTypeInfo));
    aThingTypes.add(TypeInfoFactory.intTypeInfo);
    aThingTypes.add(TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo));

    TypeInfo aThing = TypeInfoFactory.getStructTypeInfo(aThingCols, aThingTypes);
    alistExpectedTypes.add(TypeInfoFactory.getListTypeInfo(aThing));

    Assert.assertEquals(alistExpectedTypes, types);

    List<ObjectInspector> results = new ArrayList<ObjectInspector>();
    for (int i=0;i<types.size();i++){
      results.add( de.createObjectInspectorWorker(types.get(i)) );
    }

    List<ObjectInspector> alistExpectedOIResults = new ArrayList<ObjectInspector>();
    alistExpectedOIResults.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
    alistExpectedOIResults.add(ObjectInspectorFactory.getStandardListObjectInspector
            (PrimitiveObjectInspectorFactory.javaIntObjectInspector));
    alistExpectedOIResults.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

      List<ObjectInspector> colOis = new ArrayList<ObjectInspector>();
      colOis.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector);
      colOis.add( ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector));
      colOis.add( PrimitiveObjectInspectorFactory.javaIntObjectInspector);
      colOis.add( ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector));

      ObjectInspector aThingOIStruct = ObjectInspectorFactory.getStandardStructObjectInspector(aThingCols, colOis);

    alistExpectedOIResults.add(ObjectInspectorFactory.getStandardListObjectInspector(aThingOIStruct));
    Assert.assertEquals(alistExpectedOIResults, results);

  }
  
  /* Recursively walk the protobuf and map properties to type info */
  public void testTypeInfo() {
    Person.Builder bbuild = Person.newBuilder();
    bbuild.setEmail("ed@email.com").setName("ed").setId(1);
    bbuild.setHobby(Hobby.newBuilder().setName("fishing"));
    Person ed = bbuild.build();

    ProtobufDeserializer de = new ProtobufDeserializer();
    List<String> cols = new ArrayList<String>();
    List<TypeInfo> types = new ArrayList<TypeInfo>();
    de.populateTypeInfoForClass(ed.getClass(), cols, types, 0);
    ArrayList<String> expected = new ArrayList();
    expected.add("Email");
    expected.add("Hobby");
    expected.add("Id");
    expected.add("Name");

    Assert.assertEquals(expected, cols);
    
    ArrayList<TypeInfo> expectedTypes = new ArrayList<TypeInfo>();
    expectedTypes.add(TypeInfoFactory.stringTypeInfo);
    List<String> scols = Arrays.asList("Name");
    List<TypeInfo> colTypes = Arrays.asList(TypeInfoFactory.stringTypeInfo);
    expectedTypes.add(TypeInfoFactory.getStructTypeInfo(scols, colTypes));
    expectedTypes.add(TypeInfoFactory.intTypeInfo);
    expectedTypes.add(TypeInfoFactory.stringTypeInfo);

    Assert.assertEquals(expectedTypes, types);
    
  }

  public void testTypeInfoList() {
   
    AddressBook book = AddressBook.getDefaultInstance();

    ProtobufDeserializer de = new ProtobufDeserializer();
    List<String> cols = new ArrayList<String>();
    List<TypeInfo> types = new ArrayList<TypeInfo>();
    de.populateTypeInfoForClass(book.getClass(), cols, types, 0);

    ArrayList<String> expected = new ArrayList();
    expected.add("PersonCount");
    expected.add("PersonList");
    Assert.assertEquals(expected, cols);

    //string, int, string, struct<Name:string,SerializedSize:int>, int
    //array<struct<Name:string,Id:int,SerializedSize:int,Email:string,Hobby:struct<Name:string,SerializedSize:int>>>, int, int
    ArrayList<TypeInfo> expectedTypes = new ArrayList<TypeInfo>();

    List<String> hobbyCols = Arrays.asList("Name");
    List<TypeInfo> hobbyTypes = Arrays.asList(TypeInfoFactory.stringTypeInfo);
    TypeInfo hobby = TypeInfoFactory.getStructTypeInfo(hobbyCols, hobbyTypes);

    List<String> personColNames = new ArrayList<String>();
    personColNames.add("Email");
    personColNames.add("Hobby");
    personColNames.add("Id");
    personColNames.add("Name");
    

    List<TypeInfo> personTypeInfo = new ArrayList<TypeInfo>();
    personTypeInfo.add(TypeInfoFactory.stringTypeInfo);
    personTypeInfo.add(hobby);
    personTypeInfo.add(TypeInfoFactory.intTypeInfo);
    personTypeInfo.add(TypeInfoFactory.stringTypeInfo);

    TypeInfo person = TypeInfoFactory.getStructTypeInfo(personColNames, personTypeInfo);

    expectedTypes.add(TypeInfoFactory.intTypeInfo);
    expectedTypes.add(TypeInfoFactory.getListTypeInfo(person));

    Assert.assertEquals(expectedTypes, types);

  }


  public void testIsaList(){
    ArrayList l = new ArrayList();
    ProtobufDeserializer p = new ProtobufDeserializer();
    Assert.assertEquals( true , p.isaList(l.getClass()));
  }
  
  public void testOi() throws Exception {
     Person.Builder bbuild = Person.newBuilder();
    bbuild.setEmail("ed@email.com").setName("ed").setId(1);
    bbuild.setHobby(Hobby.newBuilder().setName("fishing"));
    Person ed = bbuild.build();

    ProtobufDeserializer de = new ProtobufDeserializer();
    List<String> cols = new ArrayList<String>();
    List<TypeInfo> types = new ArrayList<TypeInfo>();
    de.populateTypeInfoForClass(ed.getClass(), cols, types, 0);

    List<ObjectInspector> results = new ArrayList<ObjectInspector>();
    for (int i=0;i<types.size();i++){
      results.add( de.createObjectInspectorWorker(types.get(i)) );
    }

    List<ObjectInspector> expectedResults = new ArrayList<ObjectInspector>();
    expectedResults.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    List<String> scols = Arrays.asList("Name");
    List<ObjectInspector> stypes = new ArrayList<ObjectInspector>();
            stypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector );
            
    expectedResults.add(ObjectInspectorFactory.getStandardStructObjectInspector(scols, stypes));
    expectedResults.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
    expectedResults.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    
    
    Assert.assertEquals(expectedResults,results);
  }

  public void testAlist() throws Exception {

    AThing.Builder aThingBuild = AThing.newBuilder();
    AThing aThing = aThingBuild.addLuckynumbers(7).addLuckynumbers(4).addToys("car").build();
    AList.Builder aListBuild = AList.newBuilder();
    AList aList = aListBuild.addAge(2).addAge(3).addThings(aThing).build();


    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();

    ByteArrayOutputStream s = new ByteArrayOutputStream();
    aList.writeTo(s);
    key.set(s.toByteArray(), 0, s.toByteArray().length);
    value.set(s.toByteArray(), 0, s.toByteArray().length);

    Pair p = new Pair();
    p.setKey(key);
    p.setValue(value);


    ProtobufDeserializer de = new ProtobufDeserializer();
    de.keyClass = Ex.AList.class;
    de.valueClass = Ex.AList.class;
    de.parseFrom = de.keyClass.getMethod("parseFrom", de.byteArrayParameters);
    de.vparseFrom = de.valueClass.getMethod("parseFrom", de.byteArrayParameters);
    Object returnObj = de.deserialize(p);
    ArrayList keyVal = (ArrayList) returnObj;
    System.out.println(keyVal);
    List keyObj = (List) keyVal.get(0);
    List valObj = (List) keyVal.get(1);
   

  }

  public void testDeserialize() throws Exception {

    Person.Builder bbuild = Person.newBuilder();
    bbuild.setEmail("ed@email.com").setName("ed").setId(1);
    bbuild.setHobby(Hobby.newBuilder().setName("fishing"));
    Person ed = bbuild.build();

    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();

    ByteArrayOutputStream s = new ByteArrayOutputStream();
    ed.writeTo(s);
    key.set(s.toByteArray(), 0, s.toByteArray().length);
    value.set(s.toByteArray(), 0, s.toByteArray().length);

    Pair p = new Pair();
    p.setKey(key);
    p.setValue(value);

    ProtobufDeserializer de = new ProtobufDeserializer();
    de.keyClass = Ex.Person.class;
    de.valueClass = Ex.Person.class;
    de.parseFrom = de.keyClass.getMethod("parseFrom", de.byteArrayParameters);
    de.vparseFrom = de.valueClass.getMethod("parseFrom", de.byteArrayParameters);

    Object result = de.deserialize(p);

    


  }

  public void testCreateTable() throws Exception{
    client.execute("create table  proto   "+

  " ROW FORMAT SERDE '"+ProtobufDeserializer.class.getName()+"'"+
  " WITH SERDEPROPERTIES ('KEY_SERIALIZE_CLASS'='"+Ex.Person.class.getName()+"','VALUE_SERIALIZE_CLASS'='"+Ex.Person.class.getName()+"'   )"+
  " STORED AS INPUTFORMAT '"+SequenceFileInputFormat.class.getName()+"'"+
  " OUTPUTFORMAT '"+SequenceFileOutputFormat.class.getName()+"'" );

  client.execute("describe proto");

  List<String> results = client.fetchAll();
  List<String> expectedResults = new ArrayList<String>();
  Assert.assertEquals(2, results.size());
  expectedResults.add("key\tstruct<name:string,id:int,email:string,hobby:struct<name:string,serializedsize:int>> from deserializer");
  expectedResults.add("value\tstruct<name:string,id:int,email:string,hobby:struct<name:string,serializedsize:int>,serializedsize:int> from deserializer");
  //assertEquals(results.get(0), client.fetchAll().get(0));
  //assertEquals(results.get(1), client.fetchAll().get(1));
//  Assert.assertEquals( expectedResults, results); //whitespace issue
  client.execute("drop table proto");

  }

  public void testWithHas() throws Exception {
      Path p = new Path(ROOT_DIR, "nulstuff");
    SequenceFile.Writer w = SequenceFile.createWriter(this.getFileSystem(),
            new Configuration(), p, BytesWritable.class, BytesWritable.class);

    Ex.Person.Builder personBuilder = Ex.Person.newBuilder();
    Ex.AddressBook.Builder addressBuilder = Ex.AddressBook.newBuilder();


    //here we set pete's name but not his optional email

    Hobby.Builder h = Hobby.newBuilder();
    h.setName("tanning");
    Person pete = personBuilder
            .setId(4).setName("pete").build();
    Person stan = personBuilder.clear().setEmail("stan@prodigy.net")
            .setId(10).setName("stan").setHobby(h.build()).build();
    AddressBook b = addressBuilder.addPerson(pete).addPerson(stan).build();

    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();
    ByteArrayOutputStream s = new ByteArrayOutputStream();
    b.writeTo(s);
    ByteArrayOutputStream t = new ByteArrayOutputStream();
    b.writeTo(t);
    key.set(s.toByteArray(), 0, s.size());
    value.set(t.toByteArray(), 0, t.size());
    w.append(key, value);

    w.close();

       String jarFile;
    jarFile = KVAsVSeqFileBinaryInputFormat.class.getProtectionDomain().getCodeSource().getLocation().getFile();

    System.out.println("set hive.aux.jars.path=file:///"+jarFile);

    client.execute("add jar " + jarFile);
    client.execute("set hive.aux.jars.path=file:///"+jarFile);


    client.execute("create table  nulstuf   "
            + " ROW FORMAT SERDE '" + ProtobufDeserializer.class.getName() + "'"
            + " WITH SERDEPROPERTIES ('KEY_SERIALIZE_CLASS'='" + Ex.AddressBook.class.getName()
            + "','VALUE_SERIALIZE_CLASS'='" + Ex.AddressBook.class.getName() + "'   )"
            + " STORED AS INPUTFORMAT '" + KVAsVSeqFileBinaryInputFormat.class.getName() + "'"
            + " OUTPUTFORMAT '" + SequenceFileOutputFormat.class.getName() + "'");

    client.execute("load data local inpath '" + p.toString() + "' into table nulstuf");
    client.execute("SELECT key from nulstuf ");


    List<String> results = client.fetchAll();
    //String expectedResuls = "{\"personcount\":2,\"personlist\":[{\"email\":\"stan@prodigy.net\",\"hobby\":{\"name\":\"\"},\"id\":10,\"name\":\"stan\"},{\"email\":\"stan@prodigy.net\",\"hobby\":{\"name\":\"\"},\"id\":10,\"name\":\"stan\"}]}";

    String expectedResuls="{\"personcount\":2,\"personlist\":[{\"email\":null,\"hobby\":null,\"id\":4,\"name\":\"pete\"},{\"email\":\"stan@prodigy.net\",\"hobby\":{\"name\":\"tanning\"},\"id\":10,\"name\":\"stan\"}]}";
    //String expectedResuls= "";
    System.out.println(results.get(0));
    Assert.assertEquals(expectedResuls, results.get(0));

    client.execute("Drop table nulstuf");


  }

  public void testListWithAddressBook() throws Exception {
    Path p = new Path(ROOT_DIR, "addressbook");
    SequenceFile.Writer w = SequenceFile.createWriter(this.getFileSystem(),
            new Configuration(), p, BytesWritable.class, BytesWritable.class);
    
    Ex.Person.Builder personBuilder = Ex.Person.newBuilder();
    Ex.AddressBook.Builder addressBuilder = Ex.AddressBook.newBuilder();
    
    Person pete = personBuilder.setEmail("pete@aol.com")
            .setId(4).setName("pete").build();
    Person stan = personBuilder.clear().setEmail("stan@prodigy.net")
            .setId(10).setName("stan").build();
    AddressBook b = addressBuilder.addPerson(pete).addPerson(stan).build();
    

    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();
    ByteArrayOutputStream s = new ByteArrayOutputStream();
    b.writeTo(s);
    ByteArrayOutputStream t = new ByteArrayOutputStream();
    b.writeTo(t);
    key.set(s.toByteArray(), 0, s.size());
    value.set(t.toByteArray(), 0, t.size());
    w.append(key, value);

    w.close();

       String jarFile;
    jarFile = KVAsVSeqFileBinaryInputFormat.class.getProtectionDomain().getCodeSource().getLocation().getFile();

    System.out.println("set hive.aux.jars.path=file:///"+jarFile);

    client.execute("add jar " + jarFile);
    client.execute("set hive.aux.jars.path=file:///"+jarFile);


    client.execute("create table  address   "
            + " ROW FORMAT SERDE '" + ProtobufDeserializer.class.getName() + "'"
            + " WITH SERDEPROPERTIES ('KEY_SERIALIZE_CLASS'='" + Ex.AddressBook.class.getName()
            + "','VALUE_SERIALIZE_CLASS'='" + Ex.AddressBook.class.getName() + "'   )"
            + " STORED AS INPUTFORMAT '" + KVAsVSeqFileBinaryInputFormat.class.getName() + "'"
            + " OUTPUTFORMAT '" + SequenceFileOutputFormat.class.getName() + "'");

    client.execute("load data local inpath '" + p.toString() + "' into table address");
    client.execute("SELECT key from address ");

    List<String> results = client.fetchAll();
    //String expectedResuls = "{\"personcount\":2,\"personlist\":[{\"email\":\"stan@prodigy.net\",\"hobby\":{\"name\":\"\"},\"id\":10,\"name\":\"stan\"},{\"email\":\"stan@prodigy.net\",\"hobby\":{\"name\":\"\"},\"id\":10,\"name\":\"stan\"}]}";

    String expectedResuls="{\"personcount\":2,\"personlist\":[{\"email\":\"pete@aol.com\",\"hobby\":null,\"id\":4,\"name\":\"pete\"},{\"email\":\"stan@prodigy.net\",\"hobby\":null,\"id\":10,\"name\":\"stan\"}]}";
    //String expectedResuls= "";
    Assert.assertEquals(expectedResuls, results.get(0));


  }
 
  public void testBigTable() throws Exception {
    Path p = new Path(this.ROOT_DIR, "bigfile");
    SequenceFile.Writer w = SequenceFile.createWriter(this.getFileSystem(), new Configuration(), p, BytesWritable.class, BytesWritable.class);

    for (int i=0;i<100;i++){
      Person.Builder bbuild = Person.newBuilder();
      Person ed = bbuild.setEmail("ed@email.com").setName("ed").
              setId(i).setHobby(Hobby.newBuilder().setName("java")).build();

      Person bo = bbuild.setEmail("bo@email.com").setName("bo").
              setId(i).setHobby(Hobby.newBuilder().setName("bball")).build();

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

    String jarFile;
    jarFile = KVAsVSeqFileBinaryInputFormat.class.getProtectionDomain().getCodeSource().getLocation().getFile();

    System.out.println("set hive.aux.jars.path=file:///"+jarFile);
    
    client.execute("add jar " + jarFile);
    client.execute("set hive.aux.jars.path=file:///"+jarFile);

    client.execute("create table  bigproto   "
            + " ROW FORMAT SERDE '" + ProtobufDeserializer.class.getName() + "'"
            + " WITH SERDEPROPERTIES ('KEY_SERIALIZE_CLASS'='" + Ex.Person.class.getName()
            + "','VALUE_SERIALIZE_CLASS'='" + Ex.Person.class.getName() + "'   )"
            + " STORED AS INPUTFORMAT '" + KVAsVSeqFileBinaryInputFormat.class.getName() + "'"
            + " OUTPUTFORMAT '" + SequenceFileOutputFormat.class.getName() + "'");

    client.execute("load data local inpath '" + p.toString() + "' into table bigproto");
    client.execute("SELECT key.id, key.name FROM bigproto where key.id< 10");

    List<String> results = client.fetchAll();
    
    Assert.assertEquals("0\ted", results.get(0));
    Assert.assertEquals("1\ted", results.get(1));
    Assert.assertEquals(10, results.size());
    client.execute("drop table bigproto");

  }

  public void testCreateAndReadTable() throws Exception {
    Path p = new Path(this.ROOT_DIR, "protofile");
    SequenceFile.Writer w = SequenceFile.createWriter(this.getFileSystem(), new Configuration(), p, BytesWritable.class, BytesWritable.class);

    Person.Builder bbuild = Person.newBuilder();
    Person ed = bbuild.setEmail("ed@email.com").setName("ed").
            setId(1).setHobby(Hobby.newBuilder().setName("java")).build();

    Person bo = bbuild.setEmail("bo@email.com").setName("bo").
            setId(1).setHobby(Hobby.newBuilder().setName("bball")).build();

    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();
    ByteArrayOutputStream s = new ByteArrayOutputStream();
    ed.writeTo(s);


    ByteArrayOutputStream t = new ByteArrayOutputStream();
    bo.writeTo(t);

    key.set(s.toByteArray(), 0, s.size());
    value.set(t.toByteArray(), 0, t.size());
    w.append(key, value);
    w.close();

    client.execute("create table  protoperson   "
            + " ROW FORMAT SERDE '" + ProtobufDeserializer.class.getName() + "'"
            + " WITH SERDEPROPERTIES ('KEY_SERIALIZE_CLASS'='" + Ex.Person.class.getName()
            + "','VALUE_SERIALIZE_CLASS'='" + Ex.Person.class.getName() + "'   )"
            + " STORED AS INPUTFORMAT '" + KVAsVSeqFileBinaryInputFormat.class.getName() + "'"
            + " OUTPUTFORMAT '" + SequenceFileOutputFormat.class.getName() + "'");

    client.execute("load data local inpath '" + p.toString() + "' into table protoperson");
    client.execute("SELECT * FROM protoperson");

    List<String> results = client.fetchAll();
    String expected = "{\"email\":\"ed@email.com\",\"hobby\":{\"name\":\"java\"},\"id\":1,\"name\":\"ed\"}\t";
    expected = expected+"{\"email\":\"bo@email.com\",\"hobby\":{\"name\":\"bball\"},\"id\":1,\"name\":\"bo\"}";
    Assert.assertEquals(expected, results.get(0));
    client.execute("drop table protoperson");

  }


  public void testMatchProtoToRow() throws Exception{
      Person.Builder bbuild = Person.newBuilder();
    bbuild.setEmail("ed@email.com").setName("ed").setId(1);
    bbuild.setHobby(Hobby.newBuilder().setName("fishing"));
    Person ed = bbuild.build();

    ProtobufDeserializer de = new ProtobufDeserializer();
    List<String> cols = new ArrayList<String>();
    List<TypeInfo> types = new ArrayList<TypeInfo>();
    de.populateTypeInfoForClass(ed.getClass(), cols, types, 0);

    List<ObjectInspector> results = new ArrayList<ObjectInspector>();
    for (int i=0;i<types.size();i++){
      results.add( de.createObjectInspectorWorker(types.get(i)) );
    }
    List<Object> bla = new ArrayList<Object>();

//    de.matchProtoToRow(ed, bla, de.getObjectInspector() , cols, types);
  }


  public void testListTable() throws Exception {
    client.execute("create table  protolist   "
            + " ROW FORMAT SERDE '" + ProtobufDeserializer.class.getName() + "'"
            + " WITH SERDEPROPERTIES ('KEY_SERIALIZE_CLASS'='" + Ex.AddressBook.class.getName() + "','VALUE_SERIALIZE_CLASS'='" + Ex.AddressBook.class.getName() + "'   )"
            + " STORED AS INPUTFORMAT '" + SequenceFileInputFormat.class.getName() + "'"
            + " OUTPUTFORMAT '" + SequenceFileOutputFormat.class.getName() + "'");

    client.execute("describe protolist");
    List<String> results = client.fetchAll();
    Assert.assertEquals(2, results.size());
    //key struct<personcount:int,personlist:array<struct<email:string,hobby:struct<name:string,serializedsize:int>,id:int,name:string,serializedsize:int>>,serializedsize:int> from deserializer, value struct<personcount:int,personlist:array<struct<email:string,hobby:struct<name:string,serializedsize:int>,id:int,name:string,serializedsize:int>>,serializedsize:int> from deserializer

    String expectKey = "key\tstruct<personcount:int,personlist:array<struct<email:string,hobby:struct<name:string>,id:int,name:string>>>\tfrom deserializer";
    assertEquals(expectKey, results.get(0));

    client.execute("drop table protolist");

  }


  public void testListPrimitiveTable() throws Exception {
    client.execute("create table  protoprimlist   "
            + " ROW FORMAT SERDE '" + ProtobufDeserializer.class.getName() + "'"
            + " WITH SERDEPROPERTIES ('KEY_SERIALIZE_CLASS'='" + Ex.AList.class.getName() + "','VALUE_SERIALIZE_CLASS'='" + Ex.AList.class.getName() + "'   )"
            + " STORED AS INPUTFORMAT '" + SequenceFileInputFormat.class.getName() + "'"
            + " OUTPUTFORMAT '" + SequenceFileOutputFormat.class.getName() + "'");

    client.execute("describe protoprimlist");
    List<String> results = client.fetchAll();
    Assert.assertEquals(2, results.size());
    
    String expectKey = "key\tstruct<agecount:int,agelist:array<int>,thingscount:int,thingslist:array<struct<luckynumberscount:int,luckynumberslist:array<int>,toyscount:int,toyslist:array<string>>>>\tfrom deserializer";
    assertEquals(expectKey, results.get(0));

    client.execute("drop table protoprimlist");

  }

   public void testCreateAndReadTableList() throws Exception {
    Path p = new Path(this.ROOT_DIR, "protolistfile");
    SequenceFile.Writer w = SequenceFile.createWriter(this.getFileSystem(), new Configuration(), p, BytesWritable.class, BytesWritable.class);

    AThing.Builder aThingBuild = AThing.newBuilder();
    AThing aThing = aThingBuild.addLuckynumbers(7).addLuckynumbers(4).addToys("car").build();
    AList.Builder aListBuild = AList.newBuilder();
    AList aList = aListBuild.addAge(2).addAge(3).addThings(aThing).build();

    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();
    ByteArrayOutputStream s = new ByteArrayOutputStream();
    aList.writeTo(s);


    ByteArrayOutputStream t = new ByteArrayOutputStream();
    aList.writeTo(t);

    key.set(s.toByteArray(), 0, s.size());
    value.set(t.toByteArray(), 0, t.size());
    w.append(key, value);
    w.close();

        String jarFile;
    jarFile = KVAsVSeqFileBinaryInputFormat.class.getProtectionDomain().getCodeSource().getLocation().getFile();

    System.out.println("set hive.aux.jars.path=file:///"+jarFile);

    client.execute("add jar " + jarFile);
    client.execute("set hive.aux.jars.path=file:///"+jarFile);

    client.execute("create table  tablewithlist   "
            + " ROW FORMAT SERDE '" + ProtobufDeserializer.class.getName() + "'"
            + " WITH SERDEPROPERTIES ('KEY_SERIALIZE_CLASS'='" + Ex.AList.class.getName()
            + "','VALUE_SERIALIZE_CLASS'='" + Ex.AList.class.getName() + "'   )"
            + " STORED AS INPUTFORMAT '" + KVAsVSeqFileBinaryInputFormat.class.getName() + "'"
            + " OUTPUTFORMAT '" + SequenceFileOutputFormat.class.getName() + "'");

    client.execute("load data local inpath '" + p.toString() + "' into table tablewithlist");
    client.execute("SELECT key FROM tablewithlist");

    List<String> results = client.fetchAll();
    String expected="";
    expected = "{\"agecount\":2,\"agelist\":[2,3],\"thingscount\":1,\"thingslist\":[{\"luckynumberscount\":2,\"luckynumberslist\":[7,4],\"toyscount\":1,\"toyslist\":[\"car\"]}]}";
    Assert.assertEquals(expected, results.get(0));
    client.execute("drop table tablewithlist");

  }


  public void testNotdefined() throws Exception {
    Path p = new Path(this.ROOT_DIR, "nada");
    SequenceFile.Writer w = SequenceFile.createWriter(this.getFileSystem(), new Configuration(), p, BytesWritable.class, BytesWritable.class);

    AThing.Builder aThingBuild = AThing.newBuilder();
    AThing aThing = aThingBuild.addLuckynumbers(7).addLuckynumbers(4).addToys("car").build();
    AList.Builder aListBuild = AList.newBuilder();
    AList aList = aListBuild.addAge(2).addAge(3).addThings(aThing).build();

    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();
    ByteArrayOutputStream s = new ByteArrayOutputStream();
    aList.writeTo(s);


    ByteArrayOutputStream t = new ByteArrayOutputStream();
    aList.writeTo(t);

    key.set(s.toByteArray(), 0, s.size());
    value.set(t.toByteArray(), 0, t.size());
    w.append(key, value);
    w.close();

        String jarFile;
    jarFile = KVAsVSeqFileBinaryInputFormat.class.getProtectionDomain().getCodeSource().getLocation().getFile();

    System.out.println("set hive.aux.jars.path=file:///"+jarFile);

    client.execute("add jar " + jarFile);
    client.execute("set hive.aux.jars.path=file:///"+jarFile);

    client.execute("create table  nada   "
            + " ROW FORMAT SERDE '" + ProtobufDeserializer.class.getName() + "'"
            //+ " WITH SERDEPROPERTIES ('KEY_SERIALIZE_CLASS'='" + Ex.AList.class.getName()
            //+ "','VALUE_SERIALIZE_CLASS'='" + Ex.AList.class.getName() + "'   )"
            + " STORED AS INPUTFORMAT '" + KVAsVSeqFileBinaryInputFormat.class.getName() + "'"
            + " OUTPUTFORMAT '" + SequenceFileOutputFormat.class.getName() + "'");

    client.execute("load data local inpath '" + p.toString() + "' into table nada");
    client.execute("SELECT key FROM nada");

    List<String> results = client.fetchAll();
    String expected="null";
    //expected = "{\"agecount\":2,\"agelist\":[2,3],\"thingscount\":1,\"thingslist\":[{\"luckynumberscount\":2,\"luckynumberslist\":[7,4],\"toyscount\":1,\"toyslist\":[\"car\"]}]}";
    Assert.assertEquals(expected, results.get(0));
    client.execute("drop table nada");

  }



}
