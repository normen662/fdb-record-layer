/*
 * IFieldAccessFragment.java
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
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * An access to a field of a record: {@code x.name} or {@code x.0}. The fragment holds the ordinal of the field, its
 * position in the record type. The parser resolves a name to its ordinal, as the Cascades planner resolves the names of
 * fields to ordinals. So {@code x.name} and {@code x.1} give equal fragments if {@code name} is the field at ordinal 1.
 * The ordinal does not depend on the name: a field with the name {@code _0} can be at ordinal 1.
 *
 * <p>The record must not be nullable. The type of the access is the type of the field.</p>
 */
@API(API.Status.EXPERIMENTAL)
public final class IFieldAccessFragment implements IExpressionFragment {
    @Nonnull
    private final IExpressionFragment record;
    private final int ordinal;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates an access to a field.
     *
     * @param record the record, which must be of a record type that is not nullable
     * @param ordinal the position of the field in the record type, starting at 0
     */
    public IFieldAccessFragment(@Nonnull final IExpressionFragment record, final int ordinal) {
        this.record = Objects.requireNonNull(record);
        this.ordinal = ordinal;
        Preconditions.checkArgument(record.type() instanceof IRecordType recordType && !recordType.isNullable(),
                "a field access needs a record that is not nullable");
        Preconditions.checkArgument(ordinal >= 0 && ordinal < ((IRecordType)record.type()).fields().size(),
                "a field access needs the ordinal of a field of the record");
    }

    /**
     * {@return the record whose field the fragment accesses}
     */
    @Nonnull
    public IExpressionFragment record() {
        return record;
    }

    /**
     * {@return the position of the field in the record type, starting at 0}
     */
    public int ordinal() {
        return ordinal;
    }

    /**
     * Returns the field that the fragment accesses.
     *
     * @return the field at the ordinal of the access
     */
    @Nonnull
    public IRecordType.Field field() {
        // the constructor checks that the record is of a record type with a field at the ordinal
        return ((IRecordType)record.type()).fields().get(ordinal);
    }

    @Nonnull
    @Override
    public IType type() {
        return field().type();
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IFieldAccessFragment that && record.equals(that.record) && ordinal == that.ordinal;
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("field access", record, ordinal);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IFieldAccessFragment[record=" + record + ", ordinal=" + ordinal + "]";
    }
}
