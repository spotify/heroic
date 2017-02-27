/*
 * Copyright (c) 2016 Spotify AB.
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

package com.spotify.heroic.test;

import com.spotify.heroic.metric.QueryTrace;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import static org.hamcrest.Matchers.containsString;

public class Matchers {
    public static Matcher<QueryTrace> hasIdentifier(final Matcher<QueryTrace.Identifier> inner) {
        return new TypeSafeMatcher<QueryTrace>() {
            @Override
            protected boolean matchesSafely(final QueryTrace queryTrace) {
                return inner.matches(queryTrace.what());
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("has identifier that is ").appendDescriptionOf(inner);
            }

            @Override
            public void describeMismatchSafely(QueryTrace item, Description mismatchDescription) {
                inner.describeMismatch(item, mismatchDescription);
            }
        };
    }

    public static Matcher<QueryTrace> containsChild(final Matcher<QueryTrace> inner) {
        return new TypeSafeMatcher<QueryTrace>() {
            @Override
            protected boolean matchesSafely(final QueryTrace queryTrace) {
                return queryTrace.children().stream().anyMatch(inner::matches);
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("contains child that ").appendDescriptionOf(inner);
            }

            @Override
            public void describeMismatchSafely(QueryTrace item, Description mismatchDescription) {
                inner.describeMismatch(item.children(), mismatchDescription);
            }
        };
    }

    public static Matcher<QueryTrace.Identifier> identifierContains(final String subString) {
        final Matcher<String> stringMatcher = containsString(subString);

        return new TypeSafeMatcher<QueryTrace.Identifier>() {
            @Override
            protected boolean matchesSafely(final QueryTrace.Identifier identifier) {
                return stringMatcher.matches(identifier.getName());
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("an identifier containing ").appendValue(subString);
            }

            @Override
            public void describeMismatchSafely(
                QueryTrace.Identifier item, Description mismatchDescription
            ) {
                stringMatcher.describeMismatch(item.getName(), mismatchDescription);
            }
        };
    }
}
