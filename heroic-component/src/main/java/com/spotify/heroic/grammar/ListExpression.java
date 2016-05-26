/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.grammar;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Joiner;
import lombok.Data;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Data
@JsonTypeName("list")
public final class ListExpression implements Expression {
    private final Context context;
    private final List<Expression> list;

    @Override
    public ListExpression eval(final Scope scope) {
        final List<Expression> list = Expression.evalList(this.list, scope);
        return new ListExpression(context, list);
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {
        return visitor.visitList(this);
    }

    @Override
    public ListExpression add(Expression other) {
        final ListExpression o = other.cast(ListExpression.class);
        final ArrayList<Expression> list = new ArrayList<Expression>();
        list.addAll(this.list);
        list.addAll(o.list);
        return new ListExpression(context.join(other.getContext()), list);
    }

    @Override
    public String toRepr() {
        final Joiner arg = Joiner.on(", ");
        final Iterator<String> args = list.stream().map(Expression::toRepr).iterator();
        return "[" + arg.join(args) + "]";
    }
}
