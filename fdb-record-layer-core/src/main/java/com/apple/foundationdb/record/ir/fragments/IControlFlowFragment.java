/*
 * IControlFlowFragment.java
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

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A statement of control flow, an if, a for, a switch, or a consume: a statement with blocks that run as part of the
 * block that the statement is in. The blocks of a function, a closure, or a stream block run when the function or
 * closure is called, or as the stream is consumed, so they are not of this kind. A yield in the blocks of a
 * control-flow statement therefore belongs to the stream block that the statement is in, see
 * {@link IBlockFragment#yields()}.
 */
@API(API.Status.EXPERIMENTAL)
public sealed interface IControlFlowFragment extends IStatementFragment
        permits IIfFragment, IForFragment, ISwitchFragment, IConsumeFragment {

    /**
     * Returns the blocks of the statement, in the order in which they appear in it.
     *
     * @return the blocks
     */
    @Nonnull
    List<IBlockFragment> blocks();
}
