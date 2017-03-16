/*
 * Copyright (C) 2017 NTT DATA Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.terasoluna.batch.item.file.transform;

import org.springframework.batch.item.file.transform.AbstractLineTokenizer;
import org.springframework.batch.item.file.transform.IncorrectLineLengthException;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.util.Assert;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;

/**
 * Tokenizer used to process data obtained from files with fixed-byte-length format.
 * <p>
 * This Tokenizer differs from {@link org.springframework.batch.item.file.transform.FixedLengthTokenizer}. This Tokenizer
 * processes the value specified in the {@code ranges} as the number of bytes.
 * </p>
 *
 * @since 5.0.0
 */
public class FixedByteLengthLineTokenizer extends AbstractLineTokenizer {

    /**
     * column ranges.
     */
    private final Range[] ranges;

    /**
     * highest value within ranges.
     */
    private final int maxRange;

    /**
     * The charset to be used to convert from the bytes to the string.
     */
    private final Charset charset;

    /**
     * Set the column ranges and charset.
     * <p>
     * The ranges used in conjunction with the {@link org.springframework.batch.item.file.transform.RangeArrayPropertyEditor}
     * this property can be set in the form of a String describing the range boundaries, e.g. "1,4,7" or "1-3,4-6,7" or
     * "1-2,4-5,7-10".
     * </p>
     * <p>
     * If do not use {@link org.springframework.batch.item.file.transform.RangeArrayPropertyEditor}, max/min value of each Range
     * element must always be specified.
     * </p>
     *
     * @param ranges the column ranges expected in the input.
     * @param charset the charset to be used to convert from the bytes to the string.
     * @throws IllegalArgumentException if ranges or charset are not set.
     */
    public FixedByteLengthLineTokenizer(Range[] ranges, Charset charset) {
        Assert.notEmpty(ranges, "ranges must be set.");
        Assert.noNullElements(ranges, "elements of ranges must be set.");
        Assert.notNull(charset, "charset must be set.");
        this.ranges = ranges;
        this.maxRange = calculateMaxRange(ranges);
        this.charset = charset;
    }

    /**
     * Calculate the highest value within an array of ranges.
     * <p>
     * The ranges aren't necessarily in order. For example: "5-10, 1-4,11-15".
     * </p>
     * <p>
     * If max/min value of each Range element, cannot process correctly. For example: "1,4-20, 22".
     * </p>
     *
     * @param ranges the column ranges expected in the input.
     * @return highest value within ranges.
     * @throws IllegalArgumentException if max/min value of Range element are not set.
     */
    private int calculateMaxRange(Range[] ranges) {
        OptionalInt max = Arrays.stream(ranges).mapToInt(range -> {
            if (!range.hasMaxValue()) {
                throw new IllegalArgumentException(
                        "This range must be specified both the min and max. [range:" + range.toString() + "]");
            }
            return range.getMax();
        }).max();

        return max.orElse(0);
    }

    /**
     * Yields the tokens resulting from the splitting of the supplied {@code line} specified in {@code ranges}.
     * <p>
     * If the strict flag is true, and if line length and upper-bound of ranges specified are not equal, throw exception. If the
     * strict flag is false, and if line length is shorter than upper-bound of ranges specified, throw exception.
     * </p>
     *
     * @param line the line to be tokenized.
     * @return the resulting tokens.
     * @throws IncorrectLineLengthException If the comparison result of the line length and the upper-bound of ranges is
     *             incorrect. Comparison condition see above.
     */
    @Override
    protected List<String> doTokenize(String line) {

        byte[] lineBytes = line.getBytes(charset);

        int lineLength = lineBytes.length;

        if (lineLength != maxRange && isStrict()) {
            throw new IncorrectLineLengthException("Line length is not equal to max range. [line:" + line + "]"
                    + "[lineLength:" + lineLength + "][maxRange:" + maxRange + "]", maxRange, lineLength, line);
        }

        if (lineLength < maxRange) {
            throw new IncorrectLineLengthException("Line length is shorter than max range. [line:" + line + "]"
                    + "[lineLength:" + lineLength + "][maxRange:" + maxRange + "]", maxRange, lineLength, line);
        }

        return Arrays.stream(ranges).map(range -> new String(Arrays.copyOfRange(lineBytes, range.getMin() - 1, range
                .getMax()), charset)).collect(Collectors.toList());

    }
}
