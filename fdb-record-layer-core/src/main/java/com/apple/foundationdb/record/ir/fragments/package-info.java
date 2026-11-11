/*
 * package-info.java
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

/**
 * The fragments of FRL-IR, the objects that a program consists of: the program and its statements, which are function
 * definitions, function declarations, assignments, and expressions; the expressions, which are literals, references,
 * calls, and closures; the blocks that implement functions and closures, whose value is the value of their last
 * expression; and the types. Every fragment implements {@link com.apple.foundationdb.record.ir.fragments.IFragment},
 * and every expression is typed. The constructors reject fragments that are not well-typed, such as a call whose
 * arguments do not have the types of the parameters, or a function whose body does not end with a value of its result
 * type. The fragments depend neither on the parser nor on the planner, so a runtime can use them without either;
 * {@link com.apple.foundationdb.record.ir.cascades.CascadesTypeConverter} converts the types to and from Cascades types.
 */
package com.apple.foundationdb.record.ir.fragments;
