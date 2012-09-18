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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

import com.google.protobuf.GeneratedMessage;

//import prototest.Ex;

/* Dynamically converts serialied protobufs into nested hive types. */
public class ProtobufDeserializer implements Deserializer{

  public static final String KEY_SERIALIZE_CLASS="KEY_SERIALIZE_CLASS";
  public static final String VALUE_SERIALIZE_CLASS="VALUE_SERIALIZE_CLASS";

  Class<?> keyClass;
  Class<?> valueClass;

  private ObjectInspector oi;

  List<String> keyColumnNames = new ArrayList<String>();
  List<TypeInfo> keyColumnTypes = new ArrayList<TypeInfo>();
  List<String> valueColumnNames = new ArrayList<String>();
  List<TypeInfo> valueColumnTypes = new ArrayList<TypeInfo>();
  List<ObjectInspector> keyOIs = new ArrayList<ObjectInspector>();
  List<ObjectInspector> valueOIs = new ArrayList<ObjectInspector>();
  Class[] parameters = new Class[]{ new byte[0].getClass() };

  Map<ClassMethod,Method> cached= new HashMap<ClassMethod,Method>();

  Method parseFrom = null;
  Method vparseFrom = null;
  
  List<Object> row = new ArrayList<Object>();
  List<Object> keyRow = new ArrayList<Object>();
  List<Object> valueRow = new ArrayList<Object>();

  public ProtobufDeserializer() {
  }

  @Override
  public void initialize(Configuration job, Properties tbl) throws SerDeException {
    try {
      String keyClassName = tbl.getProperty(KEY_SERIALIZE_CLASS);
      if (keyClassName != null){
        keyClass = job.getClassByName(keyClassName);
        parseFrom = keyClass.getMethod("parseFrom", parameters);
      }
      String valueClassName = tbl.getProperty(VALUE_SERIALIZE_CLASS);
      if (valueClassName != null ){
        valueClass = job.getClassByName(valueClassName);
        vparseFrom = valueClass.getMethod("parseFrom", parameters);
      }
      this.oi= buildObjectInspector();
    } catch (Exception ex) {
      throw new SerDeException(ex.getMessage(), ex);
    }
  }

  /*
  keyRow.add("bob");
  keyRow.add(1);
  keyRow.add("bob@site");
  List<Object> hobby = new ArrayList<Object>();
  hobby.add("programming");
  hobby.add(4);
  keyRow.add(hobby);
   *
   */
  @Override
  public Object deserialize(Writable field) throws SerDeException {
    if (!(field instanceof Pair)) {
      throw new SerDeException("Writable was not an instance of Pair. It was " + field.getClass());
    }
    Pair p = (Pair) field;
    BytesWritable key = (BytesWritable) p.getKey();
    BytesWritable value = (BytesWritable) p.getValue();

    Object parsedResult = null;
    Object vparsedResult = null;
    try {
      if (parseFrom != null) {
        byte [] b = new byte [key.getLength()];
        System.arraycopy(key.getBytes(), 0, b, 0, key.getLength());
        parsedResult = parseFrom.invoke(null, b);
      }
      if (vparseFrom != null) {
        byte [] c = new byte [ value.getLength()];
        System.arraycopy(value.getBytes(), 0, c, 0, value.getLength());
        vparsedResult = vparseFrom.invoke(null, c);
      }
    } catch (IllegalAccessException ex) {
      throw new SerDeException(ex.getMessage(), ex);
    } catch (IllegalArgumentException ex) {
      throw new SerDeException(ex.getMessage(), ex);
    } catch (InvocationTargetException ex) {
      throw new SerDeException(ex.getMessage(), ex);
    }

    //key struct<name:string,id:int,email:string,hobby:struct<name:string,serializedsize:int>> from deserializer,
    //value struct<name:string,id:int,email:string,hobby:struct<name:string,serializedsize:int>,serializedsize:int> from deserializer

    row.clear();
    keyRow.clear();
    if (parseFrom !=null){
      this.matchProtoToRow(parsedResult, keyRow, keyOIs, keyColumnNames);
      row.add(keyRow);
    } else {
      row.add(null);
    }
    valueRow.clear();
    if (vparseFrom !=null){
      this.matchProtoToRow(vparsedResult, valueRow, valueOIs, valueColumnNames);
      row.add(valueRow);
    } else {
      row.add(null);
    }
    return row;
  }

  public void matchProtoToRow(Object proto, List<Object> row,
          List<ObjectInspector> ois, List<String> columnNames) {
      for (int i = 0;i<columnNames.size();i++){
       switch (ois.get(i).getCategory()){
         case PRIMITIVE:
           row.add(reflectGet(proto,columnNames.get(i)));
           //row.add(this.protoGet(proto, columnNames.get(i)));
           break;
         case LIST:
           Object listObject = reflectGet(proto,columnNames.get(i));
           ListObjectInspector li = (ListObjectInspector) ois.get(i);
           ObjectInspector subOi =li.getListElementObjectInspector();
           //System.out.println( "column name "+ columnNames.get(i) ); 
           //System.out.println(" column type "+li.getTypeName());
           if (subOi.getCategory()==Category.PRIMITIVE){
             //System.out.println(" primative");
             row.add(listObject);
             //TODO: current units do not cover this
           }
           if (subOi.getCategory() == Category.STRUCT) {
             //System.out.println(" struct");
             List x = (List) listObject;

             StructObjectInspector soi = (StructObjectInspector) subOi;
             List<? extends StructField> substructs = soi.getAllStructFieldRefs();
             List<String> subCols = new ArrayList<String>();
             List<ObjectInspector> subOis = new ArrayList<ObjectInspector>();
             for (StructField s : substructs) {
               subCols.add(s.getFieldName());
               subOis.add(s.getFieldObjectInspector());
             }
             List arrayOfStruct = new ArrayList();
             for (int it=0;it<x.size();it++){
                List<Object> subList = new ArrayList<Object>();
                //matchProtoToRow(x.get(i),subList,subOis,subCols);
                matchProtoToRow(x.get(it),subList,subOis,subCols);
                arrayOfStruct.add(subList);
             }
             row.add(arrayOfStruct);
           }
           //here
           break;
         case STRUCT:
           //row.add(null);
           Object subObject =reflectGet(proto,columnNames.get(i));
           List<Object> subList = new ArrayList<Object>();
           StructObjectInspector so = (StructObjectInspector) ois.get(i);
           List<? extends StructField> substructs = so.getAllStructFieldRefs();
           List<String> subCols = new ArrayList<String>();
           List<ObjectInspector> subOis = new ArrayList<ObjectInspector>();
           for (StructField s : substructs){
             subCols.add(s.getFieldName());
             subOis.add(s.getFieldObjectInspector());
           }
           matchProtoToRow(subObject,subList,subOis,subCols);
           
           row.add(subList);
           break;
       }

     }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return this.oi;
  }

  public ObjectInspector buildObjectInspector(){
    List<String> columnNames = new ArrayList<String>();
    columnNames.add("key");
    columnNames.add("value");
    List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>();

    keyColumnNames = new ArrayList<String>();
    keyColumnTypes = new ArrayList<TypeInfo>();

    if (this.parseFrom != null){
      populateTypeInfoForClass(this.keyClass, keyColumnNames,keyColumnTypes,0 );
    } else {
      keyColumnNames.add("undefined");
      keyColumnTypes.add(TypeInfoFactory.booleanTypeInfo);
    }
    //keyOIs = new ArrayList<ObjectInspector>();
    for(int i = 0; i < keyColumnNames.size(); i++) {
      keyOIs.add(i, createObjectInspectorWorker(keyColumnTypes.get(i)));
    }
    ObjectInspector keyOI = ObjectInspectorFactory.getStandardStructObjectInspector(keyColumnNames,keyOIs);

    valueColumnNames = new ArrayList<String>();
    valueColumnTypes = new ArrayList<TypeInfo>();
    if (this.vparseFrom != null){
      populateTypeInfoForClass(this.valueClass,valueColumnNames,valueColumnTypes,0);
    } else {
      valueColumnNames.add("undefined");
      valueColumnTypes.add(TypeInfoFactory.booleanTypeInfo);
    }
    //valueOIs = new ArrayList<ObjectInspector>();
    for(int i = 0; i < valueColumnNames.size(); i++) {
      valueOIs.add(i, createObjectInspectorWorker(valueColumnTypes.get(i)));
    }
    ObjectInspector valueOI = ObjectInspectorFactory.getStandardStructObjectInspector(valueColumnNames, valueOIs);

    columnOIs.add(keyOI);
    columnOIs.add(valueOI);
    
    ObjectInspector oi = ObjectInspectorFactory.getStandardStructObjectInspector
            (columnNames,columnOIs);
    return oi;
  }

  public void populateTypeInfoForClass(Class<?> kclass,
          List<String> columnNames,
          List<TypeInfo> columnTypes, int indent) {

    //believe it or not the order is not preserved
    //this could obliterate overloaded methods
    //but getters could not be overloaded so we are ok
    Method[] methods = kclass.getDeclaredMethods(); //getMethods()
    SortedMap<String,Method> sortedMethods = new TreeMap<String,Method>();
    for (Method m : methods ){
      sortedMethods.put(m.getName(),m);
    }
    

    for (Method m : sortedMethods.values()) {
      if (!m.getName().startsWith("get")) {
        continue;
      }
      if (m.getName().equals("getDefaultInstance")) {
        continue;
      }
      if (m.getName().equals("getDefaultInstanceForType")) {
        continue;
      }
      if (m.getName().equalsIgnoreCase("getSerializedSize")) {
    	continue;
      }
      if ( m.getParameterTypes().length != 0) {
        continue;
      }
      if (m.getReturnType().getName().contains("Descriptor")) {
        continue;
      }
      if (m.getReturnType() == null) {
        continue;
      }
      if (m.getReturnType().isPrimitive() || m.getReturnType().equals(String.class)) {
        columnNames.add(m.getName().substring(3));
        columnTypes.add(TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(m.getReturnType()));
      }
      if (m.getName().contains("OrBuilderList")){
        continue;
      }

      // list
      if (isaList(m.getReturnType())){
        //System.out.println(m +" this is a list");
        String columnName = m.getName().substring(3);
        Class listClass = null;
        Type returnType = m.getGenericReturnType();
        //System.out.println(m +" the return type "+returnType);
        if (returnType instanceof ParameterizedType){
          ParameterizedType type = (ParameterizedType) returnType;
          Type[] typeArguments = type.getActualTypeArguments();
          for(Type typeArgument : typeArguments){
            Class typeArgClass = (Class) typeArgument;
            //System.out.println("typeArgClass = " + typeArgClass);
            listClass = (Class) typeArgument;
            //System.out.println("The list class is "+listClass);
          }
        }
        if (listClass.equals( Integer.class ) || listClass.equals(String.class) 
                || listClass.equals(Long.class) || listClass.equals(Float.class)
                || listClass.equals(Double.class) || listClass.equals(Short.class)
                || listClass.equals(Byte.class) || listClass.equals(Boolean.class)){
          columnNames.add(columnName);
          columnTypes.add(TypeInfoFactory.getListTypeInfo(TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(listClass)));
        } else {

          List<String> subColumnNames = new ArrayList<String>();
          List<TypeInfo> subColumnTypes = new ArrayList<TypeInfo>();
          populateTypeInfoForClass(listClass,subColumnNames,subColumnTypes,0);
          columnNames.add(columnName);
          TypeInfo build = TypeInfoFactory.getStructTypeInfo(subColumnNames, subColumnTypes);
          columnTypes.add(TypeInfoFactory.getListTypeInfo(build));
        }
        //  handle nested list
      }

      if ( m.getReturnType().getSuperclass() != null){
        if (m.getReturnType().getSuperclass().equals(com.google.protobuf.GeneratedMessage.class)) {
          List<String> subColumnNames = new ArrayList<String>();
          List<TypeInfo> subColumnTypes = new ArrayList<TypeInfo>();
          populateTypeInfoForClass(m.getReturnType(), subColumnNames, subColumnTypes, indent + 1);
          columnNames.add(m.getName().substring(3));
          columnTypes.add(TypeInfoFactory.getStructTypeInfo(subColumnNames, subColumnTypes));
        }
      }
    }

  }

  public ObjectInspector createObjectInspectorWorker(TypeInfo ti) {
    ObjectInspector result=null;

    switch(ti.getCategory()) {
      case PRIMITIVE:
        PrimitiveTypeInfo pti = (PrimitiveTypeInfo)ti;
        result = PrimitiveObjectInspectorFactory
                .getPrimitiveJavaObjectInspector(pti.getPrimitiveCategory());
        break;
      case LIST:
        ListTypeInfo lti = (ListTypeInfo) ti;
        TypeInfo subType = lti.getListElementTypeInfo();
        result = ObjectInspectorFactory.getStandardListObjectInspector(
                createObjectInspectorWorker(subType) );
        break;
      case STRUCT:
        StructTypeInfo sti = (StructTypeInfo) ti;
        ArrayList<String> subnames = sti.getAllStructFieldNames();
        ArrayList<TypeInfo> subtypes = sti.getAllStructFieldTypeInfos();
        ArrayList<ObjectInspector> ois = new ArrayList<ObjectInspector>();
        for (int i=0;i<subtypes.size();i++){
          ois.add( createObjectInspectorWorker(subtypes.get(i)));
        }
        result = ObjectInspectorFactory.getStandardStructObjectInspector(subnames, ois);
        break;
    }
    return result;
  }

 
  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }



  public Object protoGet(Object o , String prop){
    //System.out.println("prop "+prop);
    prop = prop.toLowerCase();
    if (prop.equals("serializedsize")){
      return reflectGet(o,prop);
    }
    GeneratedMessage m = (GeneratedMessage) o;

    return m.getField( m.getDescriptorForType().findFieldByName(prop) );
  }
  
  public Object reflectGet(Object o, String prop) {

    Method m = null;
    Object result = null;
    StringBuilder sb = new StringBuilder();
    sb.append("get");
    sb.append(prop);
    ClassMethod cm = new ClassMethod(o.getClass(),sb.toString());
    m = this.cached.get(cm);
    try {
      //arge hive columns not case sensative
      //m = o.getClass().getMethod("get"+prop, new Class [0]);
      if (m==null){
        Method [] methods = o.getClass().getMethods();
        for ( int i=0;i<methods.length; i++){
          if (methods[i].getName().equalsIgnoreCase(sb.toString())){
            m = methods[i];
          }
        }
        this.cached.put(cm, m);
      }
      result = m.invoke(o, new Object[0]);
    } catch (Exception ex){
      throw new RuntimeException (ex);
    }
    return result;
  }


  public boolean isaList(Class<?> c) {
    //seems like it should work (but don't)
    // Assert.assertEquals(true,l.getClass().isAssignableFrom(List.class));
    boolean match = false;
    if (c.equals(java.util.List.class)) {
      return true;
    } else if (c.equals(java.util.Collection.class)) {
      return true;
    } if (c.equals(java.util.ArrayList.class)) {
      return true;
    }
    return match;
  }
}