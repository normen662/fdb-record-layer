/*
 * IFragment.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record.ir.fragments;

import com.apple.foundationdb.annotation.API;

/**
 * An object of FRL-IR: a program, a statement, a block, a type, and the parts they consist of. Parsing builds one
 * fragment per construct of the grammar, so this is the common result type of parsing.
 *
 * <p>Fragments are immutable values. Equality compares the parts, not the identity of the objects, and the hash code
 * of a fragment is stable: it is the same in every run of every JVM. Every implementation must override
 * {@link #equals(Object)} and {@link #hashCode()} as described there. The implementations are final classes rather
 * than records: a fragment computes its hash code on first use and keeps it, since computing it visits every fragment
 * it consists of, and a record cannot have a field besides its components.</p>
 */
@API(API.Status.EXPERIMENTAL)
public interface IFragment {

    /**
     * Returns whether the given object is a fragment of the same kind as this one whose parts are equal to the parts
     * of this one, in the same order.
     *
     * @param other the object to compare this fragment with
     * @return {@code true} if {@code other} is equal to this fragment
     */
    @Override
    boolean equals(Object other);

    /**
     * Returns a hash code that depends only on the kind of this fragment and on its parts, so that equal fragments
     * have equal hash codes in every run of every JVM. A fragment computes it with
     * {@link java.util.Objects#hash(Object...)} from a string constant that names the kind of fragment, followed by
     * the parts in the order of their declaration. Strings, numbers, booleans, lists, and optionals contribute their
     * own hash codes, whose algorithms the Java SE API specifies; a {@link com.apple.foundationdb.half.Half}
     * contributes its 16-bit pattern, which is its hash code. Fragments contribute theirs, which this method specifies.
     * An enum constant contributes the hash code of its FRL-IR keyword: {@link Enum#hashCode()} is the identity hash
     * code, which differs between runs.
     *
     * @return the hash code of this fragment
     */
    @Override
    int hashCode();
}
