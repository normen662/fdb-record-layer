/*
 * Operator.java
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

package com.apple.foundationdb.record.ir;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Optional;

/**
 * The operators of FRL-IR. An operator is a shorter way to write a call: {@code a + b} calls the function that an
 * {@code @operator("+")} annotation in scope gives to {@link #ADD}. An operator has a symbol and a number of operands.
 * The symbol {@code -} stands for two operators, {@link #SUBTRACT} with two operands and {@link #NEGATE} with one, so
 * the number of parameters of a function decides which of them its {@code @operator("-")} names.
 *
 * <p>The tabled operators {@code &&}, {@code ||}, and {@code ??} are missing: whether their right operand becomes a
 * closure, as in Swift, is not decided yet.</p>
 */
enum Operator {
    NEGATE("-", 1),
    NOT("!", 1),
    MULTIPLY("*", 2),
    DIVIDE("/", 2),
    REMAINDER("%", 2),
    ADD("+", 2),
    SUBTRACT("-", 2),
    LESS("<", 2),
    LESS_OR_EQUAL("<=", 2),
    GREATER(">", 2),
    GREATER_OR_EQUAL(">=", 2),
    EQUAL("==", 2),
    NOT_EQUAL("!=", 2);

    @Nonnull
    private final String symbol;
    private final int operandCount;

    Operator(@Nonnull final String symbol, final int operandCount) {
        this.symbol = symbol;
        this.operandCount = operandCount;
    }

    /**
     * Returns the operator of a symbol and a number of operands, as an {@code @operator} annotation names it.
     *
     * @param symbol the symbol of the operator, such as {@code +}
     * @param operandCount the number of operands, which is the number of parameters of the annotated function
     * @return the operator, or empty if FRL-IR has no such operator
     */
    @Nonnull
    static Optional<Operator> of(@Nonnull final String symbol, final int operandCount) {
        return Arrays.stream(values())
                .filter(operator -> operator.symbol.equals(symbol) && operator.operandCount == operandCount)
                .findFirst();
    }
}
