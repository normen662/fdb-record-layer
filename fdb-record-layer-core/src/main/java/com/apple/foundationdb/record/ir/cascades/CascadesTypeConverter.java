/*
 * CascadesTypeConverter.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.ir.fragments.IArrayType;
import com.apple.foundationdb.record.ir.fragments.IEnumType;
import com.apple.foundationdb.record.ir.fragments.IPrimitiveType;
import com.apple.foundationdb.record.ir.fragments.IRecordType;
import com.apple.foundationdb.record.ir.fragments.IStreamType;
import com.apple.foundationdb.record.ir.fragments.IType;
import com.apple.foundationdb.record.ir.fragments.IVectorType;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Converts between FRL-IR types and Cascades {@link Type}s.
 *
 * <p>Every Cascades type that can occur in an executable plan has an FRL-IR counterpart, and converting it to FRL-IR
 * and back yields an equal Cascades type. Record and enum names and the storage names of records, fields, and enum
 * values are not carried over; Cascades equality ignores them, and the field and enum value names, from which Cascades
 * derives storage names, are kept. Some FRL-IR types have no Cascades counterpart: {@code half} outside a vector,
 * {@code void}, type parameters of generic functions, and function types, whose signature the untyped Cascades
 * {@link Type.Function} cannot hold.</p>
 */
@API(API.Status.EXPERIMENTAL)
public final class CascadesTypeConverter {

    @Nonnull
    private static final ImmutableBiMap<IPrimitiveType.Kind, Type.TypeCode> PRIMITIVE_TYPE_CODES =
            ImmutableBiMap.<IPrimitiveType.Kind, Type.TypeCode>builder()
                    .put(IPrimitiveType.Kind.BOOLEAN, Type.TypeCode.BOOLEAN)
                    .put(IPrimitiveType.Kind.INT, Type.TypeCode.INT)
                    .put(IPrimitiveType.Kind.BIGINT, Type.TypeCode.LONG)
                    .put(IPrimitiveType.Kind.FLOAT, Type.TypeCode.FLOAT)
                    .put(IPrimitiveType.Kind.DOUBLE, Type.TypeCode.DOUBLE)
                    .put(IPrimitiveType.Kind.STRING, Type.TypeCode.STRING)
                    .put(IPrimitiveType.Kind.BYTES, Type.TypeCode.BYTES)
                    .put(IPrimitiveType.Kind.VERSION, Type.TypeCode.VERSION)
                    .build();

    /**
     * The precision that Cascades vector types state, in bits per element, for each FRL-IR vector element type.
     */
    @Nonnull
    private static final ImmutableBiMap<IPrimitiveType.Kind, Integer> VECTOR_PRECISIONS =
            ImmutableBiMap.of(IPrimitiveType.Kind.HALF, 16,
                    IPrimitiveType.Kind.FLOAT, 32,
                    IPrimitiveType.Kind.DOUBLE, 64);

    private CascadesTypeConverter() {
    }

    /**
     * Converts an FRL-IR type into the Cascades type it denotes.
     *
     * @param irType the FRL-IR type
     * @return the Cascades type
     * @throws RecordCoreException if the type has no Cascades counterpart
     */
    @Nonnull
    public static Type toCascades(@Nonnull final IType irType) {
        if (irType instanceof IPrimitiveType primitiveType) {
            if (primitiveType.kind() == IPrimitiveType.Kind.UUID) {
                return Type.uuidType(primitiveType.isNullable());
            }
            final Type.TypeCode typeCode = PRIMITIVE_TYPE_CODES.get(primitiveType.kind());
            if (typeCode != null) {
                return Type.primitiveType(typeCode, primitiveType.isNullable());
            }
        } else if (irType instanceof IRecordType recordType) {
            final ImmutableList<Type.Record.Field> fields = recordType.fields()
                    .stream()
                    .map(field -> Type.Record.Field.of(toCascades(field.type()), field.name(), field.number()))
                    .collect(ImmutableList.toImmutableList());
            return Type.Record.fromFields(recordType.isNullable(), fields);
        } else if (irType instanceof IArrayType arrayType) {
            return new Type.Array(arrayType.isNullable(), toCascades(arrayType.elementType()));
        } else if (irType instanceof IVectorType vectorType) {
            return Type.Vector.of(vectorType.isNullable(),
                    Objects.requireNonNull(VECTOR_PRECISIONS.get(vectorType.elementKind())), vectorType.dimensions());
        } else if (irType instanceof IEnumType enumType) {
            final ImmutableList<Type.Enum.EnumValue> values = enumType.values()
                    .stream()
                    .map(value -> Type.Enum.EnumValue.from(value.name(), value.number()))
                    .collect(ImmutableList.toImmutableList());
            return Type.Enum.fromValues(enumType.isNullable(), values);
        } else if (irType instanceof IStreamType streamType) {
            return new Type.Relation(toCascades(streamType.elementType()));
        }
        throw new RecordCoreException("FRL-IR type has no Cascades counterpart")
                .addLogInfo(LogMessageKeys.ACTUAL_TYPE, irType);
    }

    /**
     * Converts a Cascades type into the FRL-IR type that denotes it.
     *
     * @param type the Cascades type
     * @return the FRL-IR type
     * @throws RecordCoreException if the type has no FRL-IR counterpart, which is the case for the placeholder types of
     * the planner (e.g. the untyped function, any, and null types), erased types, and vector precisions other than
     * 16, 32, and 64 bits
     */
    @Nonnull
    public static IType fromCascades(@Nonnull final Type type) {
        if (type instanceof Type.Uuid) {
            return new IPrimitiveType(IPrimitiveType.Kind.UUID, type.isNullable());
        } else if (type instanceof Type.Primitive) {
            final IPrimitiveType.Kind kind = PRIMITIVE_TYPE_CODES.inverse().get(type.getTypeCode());
            if (kind != null) {
                return new IPrimitiveType(kind, type.isNullable());
            }
        } else if (type instanceof Type.Record recordType) {
            if (!recordType.isErased()) {
                final ImmutableList<IRecordType.Field> fields = recordType.getFields()
                        .stream()
                        .map(field -> new IRecordType.Field(fromCascades(field.getFieldType()),
                                field.getFieldNameOptional(), field.getFieldIndexOptional()))
                        .collect(ImmutableList.toImmutableList());
                return new IRecordType(recordType.isNullable(), fields);
            }
        } else if (type instanceof Type.Array arrayType) {
            if (!arrayType.isErased()) {
                return new IArrayType(arrayType.isNullable(),
                        fromCascades(Objects.requireNonNull(arrayType.getElementType())));
            }
        } else if (type instanceof Type.Vector vectorType) {
            final IPrimitiveType.Kind elementKind = VECTOR_PRECISIONS.inverse().get(vectorType.getPrecision());
            if (elementKind != null) {
                return new IVectorType(vectorType.isNullable(), elementKind, vectorType.getDimensions());
            }
        } else if (type instanceof Type.Enum enumType) {
            final ImmutableList<IEnumType.Value> values = enumType.getEnumValues()
                    .stream()
                    .map(value -> new IEnumType.Value(value.getName(), value.getNumber()))
                    .collect(ImmutableList.toImmutableList());
            return new IEnumType(enumType.isNullable(), values);
        } else if (type instanceof Type.Relation relationType) {
            if (!relationType.isErased()) {
                return new IStreamType(fromCascades(Objects.requireNonNull(relationType.getInnerType())));
            }
        }
        throw new RecordCoreException("Cascades type has no FRL-IR counterpart")
                .addLogInfo(LogMessageKeys.ACTUAL_TYPE, type);
    }
}
