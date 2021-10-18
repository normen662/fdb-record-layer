/*
 * RestaurantStoreSetup.java
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

import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

public class RestaurantStoreSetup implements StoreSetup {
    private static final Index COUNT_INDEX = new Index("globalRecordCount", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);

    @Nonnull
    @Override
    public FDBRecordStoreTestBase.RecordMetaDataHook metadataHook() {
        return (metaDataBuilder) -> {
            // Register the Restaurant record metadata
            metaDataBuilder.setRecords(TestRecords4Proto.getDescriptor());

            metaDataBuilder.addUniversalIndex(COUNT_INDEX);
            metaDataBuilder.addIndex("RestaurantRecord", "review_rating", field("reviews", KeyExpression.FanType.FanOut).nest("rating"));
            metaDataBuilder.addIndex("RestaurantRecord", "tag", field("tags", KeyExpression.FanType.FanOut).nest(
                    concatenateFields("value", "weight")));
            metaDataBuilder.addIndex("RestaurantRecord", "customers", field("customer", KeyExpression.FanType.FanOut));
            metaDataBuilder.addIndex("RestaurantRecord", "customers-name", concat(field("customer", KeyExpression.FanType.FanOut), field("name")));
            metaDataBuilder.addIndex("RestaurantReviewer", "stats$school", field("stats").nest(field("start_date")));
        };
    }

    @Override
    public void SetupStore(@Nonnull FDBRecordStore recordStore) {
        TestRecords4Proto.RestaurantReviewer.Builder reviewerBuilder = TestRecords4Proto.RestaurantReviewer.newBuilder();
        reviewerBuilder.setId(1);
        reviewerBuilder.setName("Lemuel");
        recordStore.saveRecord(reviewerBuilder.build());

        reviewerBuilder.setId(2);
        reviewerBuilder.setName("Gulliver");
        recordStore.saveRecord(reviewerBuilder.build());

        TestRecords4Proto.RestaurantRecord.Builder recBuilder = TestRecords4Proto.RestaurantRecord.newBuilder();
        recBuilder.setRestNo(101);
        recBuilder.setName("The Emperor's Three Tables");
        TestRecords4Proto.RestaurantReview.Builder reviewBuilder = recBuilder.addReviewsBuilder();
        reviewBuilder.setReviewer(1);
        reviewBuilder.setRating(10);
        reviewBuilder = recBuilder.addReviewsBuilder();
        reviewBuilder.setReviewer(2);
        reviewBuilder.setRating(3);
        TestRecords4Proto.RestaurantTag.Builder tagBuilder = recBuilder.addTagsBuilder();
        tagBuilder.setValue("Lilliput");
        tagBuilder.setWeight(5);
        recordStore.saveRecord(recBuilder.build());

        recBuilder = TestRecords4Proto.RestaurantRecord.newBuilder();
        recBuilder.setRestNo(102);
        recBuilder.setName("Small Fry's Fried Victuals");
        reviewBuilder = recBuilder.addReviewsBuilder();
        reviewBuilder.setReviewer(1);
        reviewBuilder.setRating(5);
        reviewBuilder = recBuilder.addReviewsBuilder();
        reviewBuilder.setReviewer(2);
        reviewBuilder.setRating(5);
        tagBuilder = recBuilder.addTagsBuilder();
        tagBuilder.setValue("Lilliput");
        tagBuilder.setWeight(1);
        recordStore.saveRecord(recBuilder.build());
    }
}
