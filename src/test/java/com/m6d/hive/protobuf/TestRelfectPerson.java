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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessage;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.junit.Assert;
import prototest.Ex.Hobby;
import prototest.Ex.Person;
import prototest.Ex.PersonOrBuilder;

public class TestRelfectPerson extends TestCase {

  public void testIt() {
    Class<?> recordClass = prototest.Ex.Person.class;
    Method[] methods = recordClass.getMethods();

    for (Method m : methods) {

      if (m.getName().startsWith("get")) {
        if (m.getParameterTypes().length == 0) {
          if (m.getReturnType().isPrimitive() || m.getReturnType().equals(String.class)) {
            System.out.println(" found " + m.getName().substring(3));
            //columnNames.add( m.getName().substring(3) );
            //columnTypes.add( TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(m.getReturnType()) );
          } else {
            System.out.println("Could not find inspector for " + m);
          }
        }
      }
    }

  }

  public void testGenericPropertyFromList() {

    List<Integer> i = new ArrayList<Integer>();
    TypeVariable[] a = i.getClass().getTypeParameters();
    for (TypeVariable t : a) {
      System.out.println(t);
      System.out.println(t.getGenericDeclaration());

      System.out.println(t.getName());
      Type[] types = t.getBounds();
      for (Type f : types) {
        if (f instanceof ParameterizedType) {
          ParameterizedType pt = (ParameterizedType) f;
          System.out.println("raw type " + pt.getRawType());
          System.out.println("owner type " + pt.getOwnerType());
          Type[] at = pt.getActualTypeArguments();
          for (Type bla : at) {
            System.out.println(bla);
          }
        } else {
          System.out.println("bound " + f);
        }
      }
    }
//for (int i = 0; i < genericParameterTypes.length; i++) {
//     if( genericParameterTypes[i] instanceof ParameterizedType ) {
//                Type[] parameters = ((ParameterizedType)genericParameterTypes[i]).getActualTypeArguments();
//parameters[0] contains java.lang.String for method like "method(List<String> value)"
    Assert.assertEquals(1, 1);
  }

  public void testWithoutReflection(){

     Person.Builder bbuild = Person.newBuilder();

     Person ed = bbuild.setEmail("e@aol.com").setName("ed").
              setId(3).setHobby(Hobby.newBuilder().setName("java")).build();

    GeneratedMessage m = (GeneratedMessage) ed;

    Descriptor d = m.getDescriptorForType();
    List<FieldDescriptor> fd = d.getFields();


    for (FieldDescriptor f1: fd){
      if (f1.getName().equals("id")){
        System.out.println( m.getField(f1) );
      }
    }
  }
}
