/*
 * DynamicSchema.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.dynamic;

import com.apple.foundationdb.record.query.plan.temp.Type;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * DynamicSchema.
 */
public class DynamicSchema {
    public static final DynamicSchema EMPTY_SCHEMA = empty();

    // --- public static ---

    public static DynamicSchema empty() {
        FileDescriptorSet.Builder resultBuilder = FileDescriptorSet.newBuilder();
        try {
            return new DynamicSchema(resultBuilder.build(), Maps.newHashMap());
        } catch (final DescriptorValidationException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Creates a new dynamic schema builder.
     *
     * @return the schema builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    public static DynamicSchema parseFrom(InputStream schemaDescIn) throws DescriptorValidationException, IOException {
        try {
            int len;
            byte[] buf = new byte[4096];
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while ((len = schemaDescIn.read(buf)) > 0) {
                baos.write(buf, 0, len);
            }
            return parseFrom(baos.toByteArray());
        } finally {
            schemaDescIn.close();
        }
    }

    public static DynamicSchema parseFrom(byte[] schemaDescBuf) throws DescriptorValidationException, IOException {
        return new DynamicSchema(FileDescriptorSet.parseFrom(schemaDescBuf), Maps.newHashMap());
    }

    // --- public ---

    /**
     * Creates a new dynamic message builder for the given message type.
     *
     * @param msgTypeName the message type name
     * @return the message builder (null if not found)
     */
    public DynamicMessage.Builder newMessageBuilder(String msgTypeName) {
        Descriptor msgType = getMessageDescriptor(msgTypeName);
        if (msgType == null) {
            return null;
        }
        return DynamicMessage.newBuilder(msgType);
    }

    /**
     * Creates a new dynamic message builder for the given type.
     *
     * @param type the type name
     * @return the message builder (null if not found)
     */
    public DynamicMessage.Builder newMessageBuilder(@Nonnull final Type type) {
        final String msgTypeName = typeToNameMap.get(type);
        Objects.requireNonNull(msgTypeName);
        return newMessageBuilder(msgTypeName);
    }

    public String getProtoTypeName(@Nonnull final Type type) {
        final String typeName = typeToNameMap.get(type);
        Objects.requireNonNull(typeName);
        return typeName;
    }

    /**
     * Gets the protobuf message descriptor for the given message type.
     *
     * @param msgTypeName the message type name
     * @return the message descriptor (null if not found)
     */
    public Descriptor getMessageDescriptor(String msgTypeName) {
        Descriptor msgType = msgDescriptorMapShort.get(msgTypeName);
        if (msgType == null) {
            msgType = msgDescriptorMapFull.get(msgTypeName);
        }
        return msgType;
    }

    /**
     * Gets the enum value for the given enum type and name.
     *
     * @param enumTypeName the enum type name
     * @param enumName the enum name
     * @return the enum value descriptor (null if not found)
     */
    public EnumValueDescriptor getEnumValue(String enumTypeName, String enumName) {
        EnumDescriptor enumType = getEnumDescriptor(enumTypeName);
        if (enumType == null) {
            return null;
        }
        return enumType.findValueByName(enumName);
    }

    /**
     * Gets the enum value for the given enum type and number.
     *
     * @param enumTypeName the enum type name
     * @param enumNumber the enum number
     * @return the enum value descriptor (null if not found)
     */
    public EnumValueDescriptor getEnumValue(String enumTypeName, int enumNumber) {
        EnumDescriptor enumType = getEnumDescriptor(enumTypeName);
        if (enumType == null) {
            return null;
        }
        return enumType.findValueByNumber(enumNumber);
    }

    /**
     * Gets the protobuf enum descriptor for the given enum type.
     *
     * @param enumTypeName the enum type name
     * @return the enum descriptor (null if not found)
     */
    public EnumDescriptor getEnumDescriptor(String enumTypeName) {
        EnumDescriptor enumType = enumDescriptorMapShort.get(enumTypeName);
        if (enumType == null) {
            enumType = enumDescriptorMapFull.get(enumTypeName);
        }
        return enumType;
    }

    /**
     * Returns the message types registered with the schema.
     *
     * @return the set of message type names
     */
    public Set<String> getMessageTypes() {
        return new TreeSet<>(msgDescriptorMapFull.keySet());
    }

    /**
     * Returns the enum types registered with the schema.
     *
     * @return the set of enum type names
     */
    public Set<String> getEnumTypes() {
        return new TreeSet<>(enumDescriptorMapFull.keySet());
    }

    /**
     * Returns the internal file descriptor set of this schema.
     *
     * @return the file descriptor set
     */
    public FileDescriptorSet getFileDescriptorSet() {
        return fileDescSet;
    }

    /**
     * Serializes the schema.
     *
     * @return the serialized schema descriptor
     */
    public byte[] toByteArray() {
        return fileDescSet.toByteArray();
    }

    /**
     * Returns a string representation of the schema.
     *
     * @return the schema string
     */
    public String toString() {
        Set<String> msgTypes = getMessageTypes();
        Set<String> enumTypes = getEnumTypes();
        return "types: " + msgTypes + "\nenums: " + enumTypes + "\n" + fileDescSet;
    }

    // --- private ---
    private DynamicSchema(FileDescriptorSet fileDescSet, Map<Type, String> typeToNameMap) throws DescriptorValidationException {
        this.fileDescSet = fileDescSet;
        Map<String,FileDescriptor> fileDescMap = init(fileDescSet);

        Set<String> msgDupes = new HashSet<>();
        Set<String> enumDupes = new HashSet<>();
        for (FileDescriptor fileDesc : fileDescMap.values()) {
            for (Descriptor msgType : fileDesc.getMessageTypes()) {
                addMessageType(msgType, null, msgDupes, enumDupes);
            }
            for (EnumDescriptor enumType : fileDesc.getEnumTypes()) {
                addEnumType(enumType, null, enumDupes);
            }
        }

        for (String msgName : msgDupes) {
            msgDescriptorMapShort.remove(msgName);
        }
        for (String enumName : enumDupes) {
            enumDescriptorMapShort.remove(enumName);
        }

        this.typeToNameMap = ImmutableMap.copyOf(typeToNameMap);
    }

    private Map<String,FileDescriptor> init(FileDescriptorSet fileDescSet) throws DescriptorValidationException {
        // check for dupes
        Set<String> allFdProtoNames = new HashSet<>();
        for (FileDescriptorProto fdProto : fileDescSet.getFileList()) {
            if (allFdProtoNames.contains(fdProto.getName())) {
                throw new IllegalArgumentException("duplicate name: " + fdProto.getName());
            }
            allFdProtoNames.add(fdProto.getName());
        }

        // build FileDescriptors, resolve dependencies (imports) if any
        Map<String,FileDescriptor> resolvedFileDescMap = new HashMap<>();
        while (resolvedFileDescMap.size() < fileDescSet.getFileCount()) {
            for (FileDescriptorProto fdProto : fileDescSet.getFileList()) {
                if (resolvedFileDescMap.containsKey(fdProto.getName())) {
                    continue;
                }

                List<String> dependencyList = fdProto.getDependencyList();
                List<FileDescriptor> resolvedFdList = new ArrayList<>();
                for (String depName : dependencyList) {
                    if (!allFdProtoNames.contains(depName)) {
                        throw new IllegalArgumentException("cannot resolve import " + depName + " in " + fdProto.getName());
                    }
                    FileDescriptor fd = resolvedFileDescMap.get(depName);
                    if (fd != null) {
                        resolvedFdList.add(fd);
                    }
                }

                if (resolvedFdList.size() == dependencyList.size()) { // dependencies resolved
                    FileDescriptor[] fds = new FileDescriptor[resolvedFdList.size()];
                    FileDescriptor fd = FileDescriptor.buildFrom(fdProto, resolvedFdList.toArray(fds));
                    resolvedFileDescMap.put(fdProto.getName(), fd);
                }
            }
        }

        return resolvedFileDescMap;
    }

    private void addMessageType(Descriptor msgType, String scope, Set<String> msgDupes, Set<String> enumDupes) {
        String msgTypeNameFull = msgType.getFullName();
        String msgTypeNameShort = (scope == null ? msgType.getName() : scope + "." + msgType.getName());

        if (msgDescriptorMapFull.containsKey(msgTypeNameFull)) {
            throw new IllegalArgumentException("duplicate name: " + msgTypeNameFull);
        }
        if (msgDescriptorMapShort.containsKey(msgTypeNameShort)) {
            msgDupes.add(msgTypeNameShort);
        }

        msgDescriptorMapFull.put(msgTypeNameFull, msgType);
        msgDescriptorMapShort.put(msgTypeNameShort, msgType);

        for (Descriptor nestedType : msgType.getNestedTypes()) {
            addMessageType(nestedType, msgTypeNameShort, msgDupes, enumDupes);
        }
        for (EnumDescriptor enumType : msgType.getEnumTypes()) {
            addEnumType(enumType, msgTypeNameShort, enumDupes);
        }
    }

    private void addEnumType(EnumDescriptor enumType, String scope, Set<String> enumDupes) {
        String enumTypeNameFull = enumType.getFullName();
        String enumTypeNameShort = (scope == null ? enumType.getName() : scope + "." + enumType.getName());

        if (enumDescriptorMapFull.containsKey(enumTypeNameFull)) {
            throw new IllegalArgumentException("duplicate name: " + enumTypeNameFull);
        }
        if (enumDescriptorMapShort.containsKey(enumTypeNameShort)) {
            enumDupes.add(enumTypeNameShort);
        }

        enumDescriptorMapFull.put(enumTypeNameFull, enumType);
        enumDescriptorMapShort.put(enumTypeNameShort, enumType);
    }

    private final FileDescriptorSet fileDescSet;
    private final Map<String,Descriptor> msgDescriptorMapFull = new HashMap<>();
    private final Map<String,Descriptor> msgDescriptorMapShort = new HashMap<>();
    private final Map<String,EnumDescriptor> enumDescriptorMapFull = new HashMap<>();
    private final Map<String,EnumDescriptor> enumDescriptorMapShort = new HashMap<>();
    private final Map<Type, String> typeToNameMap;

    /**
     * DynamicSchema.Builder.
     */
    public static class Builder {
        // --- public ---

        public DynamicSchema build() {
            FileDescriptorSet.Builder resultBuilder = FileDescriptorSet.newBuilder();
            resultBuilder.addFile(fileDescProtoBuilder.build());
            resultBuilder.mergeFrom(this.fileDescSetBuilder.build());
            try {
                return new DynamicSchema(resultBuilder.build(), typeToNameMap);
            } catch (final DescriptorValidationException dve) {
                throw new IllegalStateException("validation should not fail", dve);
            }
        }

        public Builder setName(String name) {
            fileDescProtoBuilder.setName(name);
            return this;
        }

        public Builder setPackage(String name) {
            fileDescProtoBuilder.setPackage(name);
            return this;
        }

        public Builder addMessageDefinition(MessageDefinition msgDef) {
            fileDescProtoBuilder.addMessageType(msgDef.getMessageType());
            return this;
        }

        public Builder addType(@Nonnull final Type type) {
            typeToNameMap.computeIfAbsent(type, t -> {
                final String protoTypeName = Type.uniqueCompliantTypeName();
                fileDescProtoBuilder.addMessageType(type.buildDescriptor(protoTypeName));
                return protoTypeName;
            });
            return this;
        }

        public Builder addEnumDefinition(EnumDefinition enumDef) {
            fileDescProtoBuilder.addEnumType(enumDef.getEnumType());
            return this;
        }

        public Builder addDependency(String dependency) {
            fileDescProtoBuilder.addDependency(dependency);
            return this;
        }

        public Builder addPublicDependency(String dependency) {
            for (int i = 0; i < fileDescProtoBuilder.getDependencyCount(); i++) {
                if (fileDescProtoBuilder.getDependency(i).equals(dependency)) {
                    fileDescProtoBuilder.addPublicDependency(i);
                    return this;
                }
            }
            fileDescProtoBuilder.addDependency(dependency);
            fileDescProtoBuilder.addPublicDependency(fileDescProtoBuilder.getDependencyCount() - 1);
            return this;
        }

        public Builder addSchema(DynamicSchema schema) {
            fileDescSetBuilder.mergeFrom(schema.fileDescSet);
            return this;
        }

        // --- private ---

        private Builder() {
            fileDescProtoBuilder = FileDescriptorProto.newBuilder();
            fileDescSetBuilder = FileDescriptorSet.newBuilder();
            typeToNameMap = Maps.newHashMap();
        }

        private final FileDescriptorProto.Builder fileDescProtoBuilder;
        private final FileDescriptorSet.Builder fileDescSetBuilder;
        private final Map<Type, String> typeToNameMap;
    }
}