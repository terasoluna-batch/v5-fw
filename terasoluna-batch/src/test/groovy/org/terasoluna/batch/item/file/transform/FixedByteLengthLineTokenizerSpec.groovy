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
package org.terasoluna.batch.item.file.transform

import org.springframework.batch.item.file.transform.IncorrectLineLengthException
import org.springframework.batch.item.file.transform.Range
import org.springframework.batch.item.file.transform.RangeArrayPropertyEditor
import spock.lang.Narrative
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.charset.Charset

/**
 * Test FixedByteLengthLineTokenizer
 *
 * @since 5.0.0
 */

@Narrative("""
Tokenize the string based on specified range as the number of bytes.
It is instantiated by specifying the ranges and charset.
""")
class FixedByteLengthLineTokenizerSpec extends Specification {

    def rangeArrayPropertyEditor = new RangeArrayPropertyEditor()
    def charset = Charset.forName("UTF-8")

    def "Constructor param 'ranges' cannot be null."() {
        when:
        new FixedByteLengthLineTokenizer(null, charset)

        then:
        def ex = thrown(IllegalArgumentException)
        ex.getMessage() == "ranges must be set."
    }

    def "Constructor param 'ranges' cannot be empty."() {
        when:
        new FixedByteLengthLineTokenizer([] as Range[], charset)

        then:
        def ex = thrown(IllegalArgumentException)
        ex.getMessage() == "ranges must be set."
    }

    def "Constructor param 'ranges' cannot have null elements."() {
        setup:
        def Range[] ranges = [new Range(1, 10), null, new Range(11, 20)]

        when:
        new FixedByteLengthLineTokenizer(ranges, charset)

        then:
        def ex = thrown(IllegalArgumentException)
        ex.getMessage() == "elements of ranges must be set."
    }

    def "Constructor param 'ranges' must have max and min value in each element."() {
        setup:
        def ranges = [new Range(1, 5), new Range(7), new Range(11, 15)] as Range[]

        when:
        new FixedByteLengthLineTokenizer(ranges, charset)

        then:
        def ex = thrown(IllegalArgumentException)
        ex.getMessage() == "This range must be specified both the min and max. [range:7]"
    }

    def "Constructor param 'charset' cannot be null."() {
        setup:
        rangeArrayPropertyEditor.setAsText("1-5, 6-10, 11-15")
        def ranges = rangeArrayPropertyEditor.getValue() as Range[]

        when:
        new FixedByteLengthLineTokenizer(ranges, null)

        then:
        def ex = thrown(IllegalArgumentException)
        ex.getMessage() == "charset must be set."
    }

    def "Constructor sets the parameters to the field"() {
        setup:
        rangeArrayPropertyEditor.setAsText("1-5, 6-10, 11-15")
        def ranges = rangeArrayPropertyEditor.getValue() as Range[]

        when:
        def fixedByteLengthLineTokenizer = new FixedByteLengthLineTokenizer(ranges, charset)

        then:
        fixedByteLengthLineTokenizer.@ranges.toString() == "[1-5, 6-10, 11-15]"
        fixedByteLengthLineTokenizer.@maxRange == 15
        fixedByteLengthLineTokenizer.@charset.name() == "UTF-8"
    }

    @Unroll
    def "calculateMaxRange method calculates the maxRange(#maxRange) from ranges(#strRanges)"() {
        setup:
        rangeArrayPropertyEditor.setAsText(strRanges)
        def ranges = rangeArrayPropertyEditor.getValue() as Range[]
        def fixedByteLengthLineTokenizer = new FixedByteLengthLineTokenizer(ranges, charset)

        expect:
        fixedByteLengthLineTokenizer.@maxRange == maxRange

        where:
        strRanges                        || maxRange
        "1-5, 6-10, 11-15, 16-20, 21-25" || 25
        "21-25, 1-5, 6-10, 11-15, 16-20" || 25
    }

    @Unroll
    def "tokenize result of line(#line) by specified range(#strRanges), charset(#charsetName) and strict(true) is tokens(#tokens)"() {
        setup:
        rangeArrayPropertyEditor.setAsText(strRanges)
        def ranges = rangeArrayPropertyEditor.getValue() as Range[]
        def charset = Charset.forName(charsetName)
        def fixedByteLengthLineTokenizer = new FixedByteLengthLineTokenizer(ranges, charset)

        expect:
        fixedByteLengthLineTokenizer.tokenize(line).toString() == tokens

        where:
        line                        | strRanges                 | charsetName || tokens
        // encoding variation
        "0123456789abcdefghij"      | "1-5, 6-10, 11-15, 16-20" | "UTF-8"     || "[01234, 56789, abcde, fghij]"
        "0123456789abcdefghij"      | "1-5, 6-10, 11-15, 16-20" | "ASCII"     || "[01234, 56789, abcde, fghij]"
        "0123456789abcdefghij"      | "1-5, 6-10, 11-15, 16-20" | "MS932"     || "[01234, 56789, abcde, fghij]"
        "0123456789abcdefghij"      | "1-5, 6-10, 11-15, 16-20" | "EUC-JP"    || "[01234, 56789, abcde, fghij]"
        // ranges variation
        "0123456789abcdefghij"      | "1-4, 7-9, 12-15, 16-20"  | "UTF-8"     || "[0123, 678, bcde, fghij]" // gap
        "0123456789abcdefghij"      | "1-5, 11-15, 6-10, 16-20" | "UTF-8"     || "[01234, abcde, 56789, fghij]" // order
        // contents variation
        "01 34  789abc   ghi "      | "1-5, 6-10, 11-15, 16-20" | "UTF-8"     || "[01 34,   789, abc  ,  ghi ]" // no-trim
        "0\r\n34567\r\na\".,e/@<>j" | "1-5, 6-10, 11-15, 16-20" | "UTF-8"     || "[0\r\n34, 567\r\n, a\".,e, /@<>j]" // control, symbol
        "    1あ  ２い  三う"       | "1-8, 9-16, 17-24"        | "UTF-8"     || "[    1あ,   ２い,   三う]" // double-byte
        "     1あ    ２い    三う"  | "1-8, 9-16, 17-24"        | "MS932"     || "[     1あ,     ２い,     三う]" // double-byte
        "1あ𠮷  ２い  三う"         | "1-8, 9-16, 17-24"        | "UTF-8"     || "[1あ𠮷,   ２い,   三う]" // surrogate pair
    }

    def "Max range of constructor param 'ranges' must be equal to the line length, if strict flag is true"() {
        setup:
        rangeArrayPropertyEditor.setAsText("1-2, 3-4, 5-6, 7-8")
        def ranges = rangeArrayPropertyEditor.getValue() as Range[]
        def fixedByteLengthLineTokenizer = new FixedByteLengthLineTokenizer(ranges, charset)

        when:
        fixedByteLengthLineTokenizer.tokenize("0123456789")

        then:
        def ex = thrown(IncorrectLineLengthException)
        ex.getMessage() == "Line length is not equal to max range. [line:0123456789][lineLength:10][maxRange:8]"
    }

    @Unroll
    def "Max range of constructor param 'ranges' may not be equal to the line length, if strict flag is false"() {
        setup:
        rangeArrayPropertyEditor.setAsText("1-5, 6-10, 11-15, 16-19")
        def ranges = rangeArrayPropertyEditor.getValue() as Range[]
        def fixedByteLengthLineTokenizer = new FixedByteLengthLineTokenizer(ranges, charset)
        fixedByteLengthLineTokenizer.setStrict(false)

        when:
        def tokens = fixedByteLengthLineTokenizer.tokenize("0123456789abcdefghij")

        then:
        tokens.toString() == "[01234, 56789, abcde, fghi]"
    }

    def "Max range of constructor param 'ranges' must be shorter than line length, if strict flag is false"() {
        setup:
        rangeArrayPropertyEditor.setAsText("1-2, 3-4, 5-6, 7-8, 9-12")
        def ranges = rangeArrayPropertyEditor.getValue() as Range[]
        def fixedByteLengthLineTokenizer = new FixedByteLengthLineTokenizer(ranges, charset)
        fixedByteLengthLineTokenizer.setStrict(false)

        when:
        fixedByteLengthLineTokenizer.tokenize("abcde12345")

        then:
        def ex = thrown(IncorrectLineLengthException)
        ex.getMessage() == "Line length is shorter than max range. [line:abcde12345][lineLength:10][maxRange:12]"
    }

}