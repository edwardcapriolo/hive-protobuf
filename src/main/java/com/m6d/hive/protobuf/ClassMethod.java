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

/*
 * Currently hive-protobuf does reflection to inspect and match nested 
 * protobufs. By saving a map of class/method name-> method we do not
 * have to inspect each row.
 */
public class ClassMethod {

  public Class clazz;
  public String method;
  
  public ClassMethod(Class c, String m) {
    this.clazz = c;
    this.method = m;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ClassMethod other = (ClassMethod) obj;
    if (this.clazz != other.clazz && (this.clazz == null || !this.clazz.equals(other.clazz))) {
      return false;
    }
    if ((this.method == null) ? (other.method != null) : !this.method.equals(other.method)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 67 * hash + (this.clazz != null ? this.clazz.hashCode() : 0);
    hash = 67 * hash + (this.method != null ? this.method.hashCode() : 0);
    return hash;
  }
 
}