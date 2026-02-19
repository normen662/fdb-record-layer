/*
 * RandomHelpers.java
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

package com.apple.foundationdb.async.common;

import javax.annotation.Nonnull;
import java.util.SplittableRandom;

public final class RandomHelpers {
    private RandomHelpers() {
        // nothing
    }

    /**
     * Seed a {@link SplittableRandom} in a deterministic way using the primary key of a record.
     * @param someIdentity something fairly unique
     * @return a new {@link SplittableRandom}
     */
    @Nonnull
    public static SplittableRandom random(@Nonnull final Object someIdentity) {
        return new SplittableRandom(splitMixLong(someIdentity.hashCode()));
    }

    /**
     * Returns a good double hash code for the argument of type {@code long}. It uses {@link #splitMixLong(long)}
     * internally and then maps the {@code long} result to a {@code double} between {@code 0} and {@code 1}.
     * @param x a {@code long}
     * @return a high quality hash code of {@code x} as a {@code double} in the range {@code [0.0d, 1.0d)}.
     */
    public static double splitMixDouble(final long x) {
        return (splitMixLong(x) >>> 11) * 0x1.0p-53;
    }

    /**
     * Returns a good long hash code for the argument of type {@code long}. It is an implementation of the
     * output mixing function {@code SplitMix64} as employed by many PRNG such as {@link SplittableRandom}.
     * See <a href="https://en.wikipedia.org/wiki/Linear_congruential_generator">Linear congruential generator</a> for
     * more information.
     * @param x a {@code long}
     * @return a high quality hash code of {@code x}
     */
    static long splitMixLong(long x) {
        x += 0x9e3779b97f4a7c15L;
        x = (x ^ (x >>> 30)) * 0xbf58476d1ce4e5b9L;
        x = (x ^ (x >>> 27)) * 0x94d049bb133111ebL;
        x = x ^ (x >>> 31);
        return x;
    }
}
