/*
 * StoreSetup.java
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

package com.apple.foundationdb.record.query.norse.setup;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;

import javax.annotation.Nonnull;

public interface StoreSetup {
    /**
     * Return the metadata creation hook. The hook should populate all metadata fields, including records.
     *
     * @return an instance of the metadata hook to invoke.
     */
    @Nonnull
    FDBRecordStoreTestBase.RecordMetaDataHook metadataHook();

    /**
     * Populate the DB with records. Assume record store is open. Do not commit.
     *
     * @param recordStore the store to use for populating the DB
     */
    void SetupStore(@Nonnull FDBRecordStore recordStore);
}
