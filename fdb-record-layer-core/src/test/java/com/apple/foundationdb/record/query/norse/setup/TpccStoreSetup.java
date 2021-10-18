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

import com.apple.foundationdb.record.TestRecordsTpccProto;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.recordType;

public class TpccStoreSetup implements StoreSetup {

    @Nonnull
    @Override
    public FDBRecordStoreTestBase.RecordMetaDataHook metadataHook() {
        return (metadataBuilder) -> {
            // Register the TPCC record metadata
            metadataBuilder.setRecords(TestRecordsTpccProto.getDescriptor());

            RecordTypeBuilder warehouse = metadataBuilder.getRecordType("Warehouse");
            RecordTypeBuilder district = metadataBuilder.getRecordType("District");
            RecordTypeBuilder customer = metadataBuilder.getRecordType("Customer");
            RecordTypeBuilder newOrder = metadataBuilder.getRecordType("NewOrder");
            RecordTypeBuilder order = metadataBuilder.getRecordType("Order");
            RecordTypeBuilder orderLine = metadataBuilder.getRecordType("OrderLine");
            RecordTypeBuilder item = metadataBuilder.getRecordType("Item");
            RecordTypeBuilder stock = metadataBuilder.getRecordType("Stock");

            // set up primary keys
            KeyExpression pkey = concat(recordType(), field("W_ID"));
            warehouse.setPrimaryKey(pkey);
            pkey = concat(recordType(), field("D_W_ID"), field("D_ID"));
            district.setPrimaryKey(pkey);
            pkey = concat(recordType(), field("C_W_ID"), field("C_D_ID"), field("C_ID"));
            customer.setPrimaryKey(pkey);
            pkey = concat(recordType(), field("NO_W_ID"), field("NO_D_ID"), field("NO_O_ID"));
            newOrder.setPrimaryKey(pkey);
            pkey = concat(recordType(), field("O_W_ID"), field("O_D_ID"), field("O_ID"));
            order.setPrimaryKey(pkey);
            pkey = concat(recordType(), field("OL_W_ID"), field("OL_D_ID"), field("OL_O_ID"), field("OL_NUMBER"));
            orderLine.setPrimaryKey(pkey);
            pkey = concat(recordType(), field("I_ID"));
            item.setPrimaryKey(pkey);
            pkey = concat(recordType(), field("S_W_ID"), field("S_I_ID"));
            stock.setPrimaryKey(pkey);
        };
    }

    @Override
    public void SetupStore(@Nonnull FDBRecordStore recordStore) {
        TestRecordsTpccProto.Warehouse.Builder warehouse = createWarehouse(13, "Warehouse 13", "1 Main st.", "Springfield", "QU", "00000");
        recordStore.saveRecord(warehouse.build());

        TestRecordsTpccProto.District.Builder district = createDistrict(1, 13, "District 1", "1 Main st.", "Springfield", "QU", "00000", 10, 100, 21);
        recordStore.saveRecord(district.build());

        district = createDistrict(9, 13, "District 9", "1 Side st.", "Blah", "QU", "00001", 10, 100, 21);
        recordStore.saveRecord(district.build());

        TestRecordsTpccProto.History.Builder historyBuilder = createHistory(1, 9, 13, 9, 13);
        List<TestRecordsTpccProto.History> history = Collections.singletonList(historyBuilder.build());

        TestRecordsTpccProto.Customer.Builder customer = createCustomer(1, 9, 13, "First", "Middle", "Last", "20 Washington st.", "Washington", "WA", "13323", "555-555-5555", 22441166, "GC", 1000, 15, 34, 1, 1, 1, "Nice fellow", history);
        recordStore.saveRecord(customer.build());
    }

    @Nonnull
    private TestRecordsTpccProto.Warehouse.Builder createWarehouse(final int id, final String name, final String address1, final String city, final String state, final String zip) {
        TestRecordsTpccProto.Warehouse.Builder warehouseBuilder = TestRecordsTpccProto.Warehouse.newBuilder();
        warehouseBuilder.setWID(id);
        warehouseBuilder.setWNAME(name);
        warehouseBuilder.setWSTREET1(address1);
        warehouseBuilder.setWCITY(city);
        warehouseBuilder.setWSTATE(state);
        warehouseBuilder.setWZIP(zip);
        return warehouseBuilder;
    }

    @Nonnull
    private TestRecordsTpccProto.District.Builder createDistrict(final int id, final int warehouse, final String name, final String address1, final String city, final String state, final String zip, final int tax, final int ytd, final int nextOrderId) {
        TestRecordsTpccProto.District.Builder districtBuilder = TestRecordsTpccProto.District.newBuilder();
        districtBuilder.setDID(id);
        districtBuilder.setDWID(warehouse);
        districtBuilder.setDNAME(name);
        districtBuilder.setDSTREET1(address1);
        districtBuilder.setDCITY(city);
        districtBuilder.setDSTATE(state);
        districtBuilder.setDZIP(zip);
        districtBuilder.setDTAX(tax);
        districtBuilder.setDYTD(ytd);
        districtBuilder.setDNEXTOID(nextOrderId);
        return districtBuilder;
    }

    @Nonnull
    private TestRecordsTpccProto.Customer.Builder createCustomer(final long id, final long district, final long warehouse, final String first, final String middle, final String last, final String address1, final String city, final String state, final String zip, final String phone, final long since, final String credit, final int creditLim, final int discount, final int balance, final int ytdPayment, final int paymentCount, final int deliveryCount, final String data, final Iterable<TestRecordsTpccProto.History> history) {
        TestRecordsTpccProto.Customer.Builder customerBuilder = TestRecordsTpccProto.Customer.newBuilder();
        customerBuilder.setCID(id);
        customerBuilder.setCDID(district);
        customerBuilder.setCWID(warehouse);
        customerBuilder.setCFIRST(first);
        customerBuilder.setCMIDDLE(middle);
        customerBuilder.setCLAST(last);
        customerBuilder.setCSTREET1(address1);
        customerBuilder.setCCITY(city);
        customerBuilder.setCSTATE(state);
        customerBuilder.setCZIP(zip);
        customerBuilder.setCPHONE(phone);
        customerBuilder.setCSINCE(since);
        customerBuilder.setCCREDIT(credit);
        customerBuilder.setCCREDITLIM(creditLim);
        customerBuilder.setCDISCOUNT(discount);
        customerBuilder.setCBALANCE(balance);
        customerBuilder.setCYTDPAYMENT(ytdPayment);
        customerBuilder.setCPAYMENTCNT(paymentCount);
        customerBuilder.setCDELIVERYCNT(deliveryCount);
        customerBuilder.setCDATA(data);
        customerBuilder.addAllCHISTORY(history);
        return customerBuilder;
    }

    @Nonnull
    private TestRecordsTpccProto.History.Builder createHistory(final long customer, final long customerDistrict, final long customerWarehouse, final long district, final long districtWarehouse) {
        TestRecordsTpccProto.History.Builder historyBuilder = TestRecordsTpccProto.History.newBuilder();
        historyBuilder.setHCID(customer);
        historyBuilder.setHCDID(customerDistrict);
        historyBuilder.setHCWID(customerWarehouse);
        historyBuilder.setHDID(district);
        historyBuilder.setHWID(districtWarehouse);
        return historyBuilder;
    }
}
