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
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;

/* Dynamically converts serialied protobufs into nested hive types. */
public class ProtobufDeserializer implements Deserializer{

  public static final String KEY_SERIALIZE_CLASS = "KEY_SERIALIZE_CLASS";
  public static final String VALUE_SERIALIZE_CLASS = "VALUE_SERIALIZE_CLASS";

  public static final String KEY = "key";
  public static final String VALUE = "value";
  public static final String UNDEFINED = "undefined";
  public static final String PARSE_FROM = "parseFrom";
  
  private static final class NoSuchMethod {
	@SuppressWarnings("unused") public final void none() {}
  }
  public static final Method NO_SUCH_METHOD; 
  static {
	  try {
		  NO_SUCH_METHOD = NoSuchMethod.class.getDeclaredMethod("none");
	  } catch(NoSuchMethodException e) {
		  throw new RuntimeException("Can't happen", e);
	  }
  }

  Class<?> keyClass;
  Class<?> valueClass;

  private ObjectInspector oi;

  List<String> keyColumnNames = new ArrayList<String>();
  List<TypeInfo> keyColumnTypes = new ArrayList<TypeInfo>();
  List<String> valueColumnNames = new ArrayList<String>();
  List<TypeInfo> valueColumnTypes = new ArrayList<TypeInfo>();
  List<ObjectInspector> keyOIs = new ArrayList<ObjectInspector>();
  List<ObjectInspector> valueOIs = new ArrayList<ObjectInspector>();

  Map<Class<?>, Map<String, Method>> cached= new HashMap<Class<?>, Map<String,Method>>();
  Map<Class<?>, Map<String, Method>> cachedHas= new HashMap<Class<?>, Map<String, Method>>();
  Map<ClassMethod,FieldDescriptor> protoCache= new HashMap<ClassMethod,FieldDescriptor>();
  Map<ClassMethod,FieldDescriptor> protoHasCache= new HashMap<ClassMethod,FieldDescriptor>();


  Method parseFrom = null;
  Method vparseFrom = null;
  
  List<Object> row = new ArrayList<Object>();
  List<Object> keyRow = new ArrayList<Object>();
  List<Object> valueRow = new ArrayList<Object>();

  public static final Object[] noArgs = new Object [0];
  public static final Class<?>[] byteArrayParameters = new Class[]{ new byte[0].getClass() };
  
  public ProtobufDeserializer() { 
  }

  @Override
  public void initialize(Configuration job, Properties tbl) throws SerDeException {
    try {
      String keyClassName = tbl.getProperty(KEY_SERIALIZE_CLASS);
      if (keyClassName != null){
        keyClass = job.getClassByName(keyClassName);
        parseFrom = keyClass.getMethod(PARSE_FROM, byteArrayParameters);
      }
      String valueClassName = tbl.getProperty(VALUE_SERIALIZE_CLASS);
      if (valueClassName != null ){
        valueClass = job.getClassByName(valueClassName);
        vparseFrom = valueClass.getMethod(PARSE_FROM, byteArrayParameters);
      }
      this.oi= buildObjectInspector();
    } catch (Exception ex) {
      throw new SerDeException(ex.getMessage(), ex);
    }
  }

  @Override
  public Object deserialize(Writable field) throws SerDeException {
    //if (!(field instanceof Pair)) {
    //  throw new SerDeException("Writable was not an instance of Pair. It was " + field.getClass());
    //}
    BytesWritable key = null;
    BytesWritable value = null;
    if (field instanceof Pair){
      Pair p = (Pair) field;
      key = (BytesWritable) p.getKey();
      value = (BytesWritable) p.getValue();
    } else if (field instanceof BytesWritable){
      parseFrom = null;
      value = (BytesWritable) field;
    }
    Message parsedResult = null;
    Message vparsedResult = null;
    try {
      if (parseFrom != null) {
        byte [] b = new byte [key.getLength()];
        System.arraycopy(key.getBytes(), 0, b, 0, key.getLength());
        parsedResult = (Message)parseFrom.invoke(null, b);
      }
      if (vparseFrom != null) {
        byte [] c = new byte [ value.getLength()];
        System.arraycopy(value.getBytes(), 0, c, 0, value.getLength());
        vparsedResult = (Message)vparseFrom.invoke(null, c);
      }
    } catch (IllegalAccessException ex) {
      throw new SerDeException(ex.getMessage(), ex);
    } catch (IllegalArgumentException ex) {
      throw new SerDeException(ex.getMessage(), ex);
    } catch (InvocationTargetException ex) {
      throw new SerDeException(ex.getMessage(), ex);
    }

    row.clear();
    keyRow.clear();
    if (parseFrom !=null){
      try {
        this.matchProtoToRow(parsedResult, keyRow, keyOIs, keyColumnNames);
      } catch (Exception ex) {
        throw new SerDeException(ex);
      }
      row.add(keyRow);
    } else {
      row.add(null);
    }
    valueRow.clear();
    if (vparseFrom !=null){
      try {
        this.matchProtoToRow(vparsedResult, valueRow, valueOIs, valueColumnNames);
      } catch (Exception ex) {
        throw new SerDeException(ex);
      }
      row.add(valueRow);
    } else {
      row.add(null);
    }
    return row;
  }

  public void matchProtoToRow(Message proto, List<Object> row, List<ObjectInspector> ois, List<String> columnNames) throws Exception{
    for (int i = 0;i<columnNames.size();i++) {
      matchProtoToRowField(proto, row, ois.get(i), columnNames.get(i));
    }
  }
  
  public void matchProtoToRow(Message proto, List<Object> row, List<? extends StructField> structFields) throws Exception {
	for (int i = 0;i<structFields.size();i++) {
	  ObjectInspector oi = structFields.get(i).getFieldObjectInspector();
	  String colName = structFields.get(i).getFieldName();
	  matchProtoToRowField(proto, row, oi, colName);
	}
  }
  
  private void matchProtoToRowField(Message m, List<Object> row, ObjectInspector oi, String colName) throws Exception {
    switch (oi.getCategory()){
      case PRIMITIVE:
        if (this.reflectHas(m,colName)){
          row.add(reflectGet(m,colName));
        } else {
          row.add(null);
        }
        break;
      case LIST:
        /* there is no hasList in proto a null list is an empty list.
         * no need to call hasX
        */
        Object listObject = reflectGet(m,colName);
        ListObjectInspector li = (ListObjectInspector) oi;
        ObjectInspector subOi =li.getListElementObjectInspector();
        if (subOi.getCategory()==Category.PRIMITIVE){
          row.add(listObject);
        } else if (subOi.getCategory() == Category.STRUCT) {
          @SuppressWarnings("unchecked")
		  List<Message> x = (List<Message>) listObject;
          StructObjectInspector soi = (StructObjectInspector) subOi;
          List<? extends StructField> substructs = soi.getAllStructFieldRefs();
          List<List<?>> arrayOfStruct = new ArrayList<List<?>>(x.size());
          for (int it=0;it<x.size();it++){
            List<Object> subList = new ArrayList<Object>(substructs.size());
            matchProtoToRow(x.get(it),subList,substructs);
            arrayOfStruct.add(subList);
          }
          row.add(arrayOfStruct);
        } else {
          //never should happen
        }
        break;
      case STRUCT:
        if (this.reflectHas(m, colName)){
          Message subObject = (Message)reflectGet(m, colName);
          StructObjectInspector so = (StructObjectInspector) oi;
          List<? extends StructField> substructs = so.getAllStructFieldRefs();
          List<Object> subList = new ArrayList<Object>(substructs.size());
          matchProtoToRow(subObject,subList,substructs);
          row.add(subList);
        } else {
          row.add( null);
        }
        break;
    }	  
  }
  


  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return this.oi; 
  }

  public ObjectInspector buildObjectInspector(){
    List<String> columnNames = new ArrayList<String>();
    columnNames.add(KEY);
    columnNames.add(VALUE);
    List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>();

    keyColumnNames = new ArrayList<String>();
    keyColumnTypes = new ArrayList<TypeInfo>();
    if (this.parseFrom != null){
      populateTypeInfoForClass(this.keyClass, keyColumnNames,keyColumnTypes,0);
    } else {
      keyColumnNames.add(UNDEFINED);
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
      valueColumnNames.add(UNDEFINED);
      valueColumnTypes.add(TypeInfoFactory.booleanTypeInfo);
    }
    //valueOIs = new ArrayList<ObjectInspector>();
    for(int i = 0; i < valueColumnNames.size(); i++) {
      valueOIs.add(i, createObjectInspectorWorker(valueColumnTypes.get(i)));
    }
    ObjectInspector valueOI = ObjectInspectorFactory.getStandardStructObjectInspector(valueColumnNames, valueOIs);

    columnOIs.add(keyOI);
    columnOIs.add(valueOI);
    
    ObjectInspector oi = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames,columnOIs);
    return oi;
  }

  public void populateTypeInfoForClass(Class<?> kclass,
          List<String> columnNames,
          List<TypeInfo> columnTypes, 
          int indent) {

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
      if ( m.getParameterTypes().length != 0) {
        continue;
      }
      if (m.getReturnType() == null) {
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
      
      if (m.getReturnType().getName().contains("Descriptor")) {
        continue;
      }
      
      if (m.getReturnType().isPrimitive() || m.getReturnType().equals(String.class)) {
        columnNames.add(m.getName().substring(3));
        columnTypes.add(TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(m.getReturnType()));
      }
      if (m.getName().contains("OrBuilderList")){
        continue;
      }
      
      if (isaList(m.getReturnType())){
        String columnName = m.getName().substring(3);
        Class<?> listClass = null;
        Type returnType = m.getGenericReturnType();
        if (returnType instanceof ParameterizedType){
          ParameterizedType type = (ParameterizedType) returnType;
          Type[] typeArguments = type.getActualTypeArguments();
          for(Type typeArgument : typeArguments){
            listClass = (Class<?>) typeArgument;
          }
        }
        if (protoPrimitives.contains(listClass)) {
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
        //nested list not possible with protobuf
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
  
  private static Set<Class<?>> protoPrimitives = Sets.<Class<?>>newHashSet(
	Boolean.class, 
	Byte.class, Short.class, Integer.class, Long.class,
	Float.class, Double.class,
	String.class
  );
  

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

  public Object protoGet(Object o , String prop) throws Exception{
    prop = prop.toLowerCase();
    if (prop.equals("serializedsize")){
      return reflectGet(o,prop);
    }
    GeneratedMessage m = (GeneratedMessage) o;
    return m.getField( m.getDescriptorForType().findFieldByName(prop) );
  }

  public Object protoCacheGet(Message m, String prop) throws Exception{
    prop = prop.toLowerCase();
    if (prop.endsWith("count")){
      return reflectGet(m,prop);
    }
    ClassMethod cm = new ClassMethod(m.getClass(),prop);
    FieldDescriptor f =this.protoCache.get(cm);
    if (f == null){
      f = m.getDescriptorForType().findFieldByName(prop);
      this.protoCache.put(cm, f);
    }
    return m.getField(f);
  } 

  public boolean protoHas(Message m , String prop) throws Exception {
    prop=prop.toLowerCase();
    if (prop.contains("count")){
      return reflectHas(m,prop);
    }
    ClassMethod meth = new ClassMethod(m.getClass(),prop);
    FieldDescriptor f =protoHasCache.get(meth);
    if (f == null){
      f = m.getDescriptorForType().findFieldByName(prop);
      protoHasCache.put(meth, f);
    }
    return m.hasField(f);
  }
  
  public Method findMethodIgnoreCase(Class<?> cls, String methodName) {
	Method[] methods = cls.getMethods();
	for (int i=0;i<methods.length;i++){
	  if (methods[i].getName().equalsIgnoreCase(methodName)){
	    return methods[i];
	  }
	}
	return NO_SUCH_METHOD;
  }
  
  public Map<String, Method> getCachedSubMap(Map<Class<?>, Map<String, Method>> cache, Class<?> cls) {
	  Map<String, Method> classMap = cache.get(cls);
	  if(classMap == null) {
		  classMap = new HashMap<String, Method>();
		  cache.put(cls, classMap);
	  }
	  return classMap;
  }
  
  public boolean reflectHas(Object o, String prop) throws Exception{
	final Class<?> cls = o.getClass();
	final Map<String, Method> classMap = getCachedSubMap(this.cachedHas, cls);
    Method m = classMap.get(prop);
    if (m==null){
      m = findMethodIgnoreCase(cls, "has"+prop);
      classMap.put(prop, m);
    }
    //Methods like xCount (which need to be removed) are artifically generated
    //thus we say they always exist for now
    if (m == NO_SUCH_METHOD){
      return true;
    } else {
      Object result = m.invoke(o, noArgs);
      return (Boolean.TRUE.equals(result));
    }
  }

  public Object reflectGet(Object o, String prop) throws Exception{
    final Class<?> cls = o.getClass();
    final Map<String, Method> classMap = getCachedSubMap(this.cached, cls);
    Method m = classMap.get(prop); 
    if (m==null){
      m = findMethodIgnoreCase(cls, "get"+prop);
      classMap.put(prop, m);
    }
    Object result = m.invoke(o, noArgs);
    return result;
  }

  public boolean isaList(Class<?> c) {
    if (c.equals(java.util.ArrayList.class)) {
      return true;
    } else if (c.equals(java.util.List.class)) {
      return true;
    } else if (c.equals(java.util.Collection.class)) {
      return true;
    } else {
      return false; 
    }
  }
}
