/*
 * GrpcConstants.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.jdbc.grpc;

import com.apple.foundationdb.annotation.API;

/**
 * Constants for consumers of GRPC such as the default port
 * the Relational server listens for connections on.
 */
@API(API.Status.EXPERIMENTAL)
public class GrpcConstants {
    /**
     * Default port the GRPC server listens on.
     */
    public static final int DEFAULT_SERVER_PORT = 1111;
}
