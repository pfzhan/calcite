/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

/**
 * UDF implementor that looks up target method on given class as the implementation
 * note that this implementor will always pass operands as boxed types.
 */
public class UdfMethodNameImplementor implements CallImplementor {
  private String methodName;
  private Class<?> clazz;

  public UdfMethodNameImplementor(String methodName, Class<?> clazz) {
    this.methodName = methodName;
    this.clazz = clazz;
  }

  @Override public Expression implement(
          RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
    List<Expression> opExps = new ArrayList<>(call.getOperands().size());
    for (RexNode opRex : call.getOperands()) {
      opExps.add(
          Expressions.convert_(
              translator.translate(opRex, nullAs),
              translator.typeFactory.getJavaClass(
                  translator.typeFactory.createTypeWithNullability(opRex.getType(), true))));
    }
    return Expressions.call(
        clazz,
        methodName,
        opExps);
  }
}
