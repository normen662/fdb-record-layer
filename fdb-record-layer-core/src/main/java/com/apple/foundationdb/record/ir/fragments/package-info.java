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
 * definitions and declarations, overloaded functions, type declarations, type aliases, the package statement,
 * assignments, yields and yield breaks, the statements of control flow (if, for, and consume), and expressions; the
 * expressions, which are literals, references, calls, field accesses, record constructors, closures, and stream blocks;
 * the blocks that implement functions, closures, and stream blocks, and the blocks of the statements of control flow,
 * whose value is the value of their last expression; the types; and the annotations that say how a package, a
 * function, or an extern type lives outside FRL-IR. Every fragment implements
 * {@link com.apple.foundationdb.record.ir.fragments.IFragment}, and every expression is typed. A reference to a
 * function, and an extern type, refer back to the fragment that defines or declares them, while a type alias stands for
 * its type, so no type refers to it. A variant of an overloaded function refers to the overloaded function, and a
 * reference to an overloaded function refers to the variant that the parser selected. An operator is a call, so no
 * fragment stands for it. The constructors reject fragments that are not well-typed, such as a call whose arguments do
 * not have the types of the parameters, or a function whose body does not end with a value of its result type. The
 * fragments depend neither on the parser nor on the planner, so a runtime can use them without either;
 * {@link com.apple.foundationdb.record.ir.cascades.CascadesTypeConverter} converts the types to and from Cascades
 * types.
 */
package com.apple.foundationdb.record.ir.fragments;
