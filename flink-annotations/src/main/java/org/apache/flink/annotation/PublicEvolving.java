/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * Annotation to mark classes and methods for public use, but with evolving interfaces.
 * 标记类和方法用于共用,但是具有进化接口的注解
 *
 * <p>Classes and methods with this annotation are intended for public use and have stable behavior.
 * However, their interfaces and signatures are not considered to be stable and might be changed
 * across versions.
 * 添加了该注解的类和方法打算给公共调用使用,具有稳定的行为。
 * 但是他们的接口和签名课鞥不稳定,在版本减可能会变化。
 *
 * <p>This annotation also excludes methods and classes with evolving interfaces / signatures
 * within classes annotated with {@link Public}.
 * 该注释还不包括在使用{@link Public}注释的类中具有演进的接口/签名的方法和类
 *
 */
@Documented
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.CONSTRUCTOR })
@Public
public @interface PublicEvolving {
}
