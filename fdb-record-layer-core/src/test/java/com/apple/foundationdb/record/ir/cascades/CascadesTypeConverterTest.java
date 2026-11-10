/*
 * CascadesTypeConverterTest.java
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

package com.apple.foundationdb.record.ir.cascades;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.ir.types.IFunctionType;
import com.apple.foundationdb.record.ir.types.IGenericType;
import com.apple.foundationdb.record.ir.types.IPrimitiveType;
import com.apple.foundationdb.record.ir.types.IType;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.ir.FrlIrParsing.parseType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link CascadesTypeConverter}: Cascades types survive the conversion to FRL-IR and back, both as objects
 * and through FRL-IR text.
 */
class CascadesTypeConverterTest {
    private static final Type LONG = Type.primitiveType(Type.TypeCode.LONG, false);
    private static final Type NULLABLE_STRING = Type.primitiveType(Type.TypeCode.STRING, true);

    @Nonnull
    static Stream<Type> cascadesTypes() {
        return Stream.of(
                Type.primitiveType(Type.TypeCode.BOOLEAN, false),
                Type.primitiveType(Type.TypeCode.INT, true),
                LONG,
                Type.primitiveType(Type.TypeCode.FLOAT, false),
                Type.primitiveType(Type.TypeCode.DOUBLE, true),
                NULLABLE_STRING,
                Type.primitiveType(Type.TypeCode.BYTES, false),
                Type.primitiveType(Type.TypeCode.VERSION, false),
                Type.uuidType(true),
                // field numbers that do not follow the field order, as after a field has been removed from a schema
                Type.Record.fromFields(false, ImmutableList.of(
                        Type.Record.Field.of(LONG, Optional.of("ID"), Optional.of(1)),
                        Type.Record.Field.of(NULLABLE_STRING, Optional.of("FNAME"), Optional.of(2)),
                        Type.Record.Field.of(NULLABLE_STRING, Optional.of("LNAME"), Optional.of(4)))),
                // unnamed fields, which Cascades names and numbers by position
                Type.Record.fromFields(true, ImmutableList.of(
                        Type.Record.Field.unnamedOf(LONG),
                        Type.Record.Field.unnamedOf(NULLABLE_STRING))),
                // names that FRL-IR text writes between backticks
                Type.Record.fromFields(false, ImmutableList.of(
                        Type.Record.Field.of(LONG, Optional.of("version"), Optional.of(1)),
                        Type.Record.Field.of(NULLABLE_STRING, Optional.of("a.b"), Optional.of(2)),
                        Type.Record.Field.of(LONG, Optional.of("a$b"), Optional.of(3)))),
                Type.Enum.fromValues(false, ImmutableList.of(
                        Type.Enum.EnumValue.from("enum", 0),
                        Type.Enum.EnumValue.from("a.b", 1))),
                // a record type derived from a protobuf message, including a repeated field
                Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor()),
                new Type.Array(true, Type.Record.fromFields(false, ImmutableList.of(
                        Type.Record.Field.of(LONG, Optional.of("ID"), Optional.of(1))))),
                Type.Vector.of(false, 16, 10),
                Type.Vector.of(true, 32, 3),
                Type.Vector.of(false, 64, 512),
                Type.Enum.fromValues(true, ImmutableList.of(
                        Type.Enum.EnumValue.from("UNLOCKED", 1),
                        Type.Enum.EnumValue.from("WRITE_LOCKED", 2),
                        Type.Enum.EnumValue.from("RETIRED", 3))),
                new Type.Relation(Type.Record.fromFields(false, ImmutableList.of(
                        Type.Record.Field.of(LONG, Optional.of("ID"), Optional.of(1))))));
    }

    @ParameterizedTest
    @MethodSource("cascadesTypes")
    void fromCascadesThenToCascadesYieldsEqualType(@Nonnull final Type type) {
        assertThat(CascadesTypeConverter.toCascades(CascadesTypeConverter.fromCascades(type))).isEqualTo(type);
    }

    @ParameterizedTest
    @MethodSource("cascadesTypes")
    void fromCascadesThroughTextThenToCascadesYieldsEqualType(@Nonnull final Type type) {
        final String text = CascadesTypeConverter.fromCascades(type).toString();
        assertThat(CascadesTypeConverter.toCascades(parseType(text))).isEqualTo(type);
    }

    @Nonnull
    static Stream<IType> irTypesWithoutCounterpart() {
        final IType intType = new IPrimitiveType(IPrimitiveType.Kind.INT, false);
        return Stream.of(new IPrimitiveType(IPrimitiveType.Kind.HALF, false),
                new IPrimitiveType(IPrimitiveType.Kind.VOID, false),
                new IGenericType("T", false),
                new IFunctionType(false, ImmutableList.of(intType), intType));
    }

    @ParameterizedTest
    @MethodSource("irTypesWithoutCounterpart")
    void toCascadesTypeWithoutCounterpartFails(@Nonnull final IType type) {
        assertThatThrownBy(() -> CascadesTypeConverter.toCascades(type)).isInstanceOf(RecordCoreException.class);
    }

    @Nonnull
    static Stream<Type> cascadesTypesWithoutCounterpart() {
        return Stream.of(Type.FUNCTION, new Type.Array(), new Type.Relation(), Type.Vector.of(false, 8, 3));
    }

    @ParameterizedTest
    @MethodSource("cascadesTypesWithoutCounterpart")
    void fromCascadesTypeWithoutCounterpartFails(@Nonnull final Type type) {
        assertThatThrownBy(() -> CascadesTypeConverter.fromCascades(type)).isInstanceOf(RecordCoreException.class);
    }
}
