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
package org.terasoluna.batch.item.file

import org.springframework.batch.item.file.transform.IncorrectLineLengthException
import org.springframework.core.io.ByteArrayResource
import org.springframework.core.io.InputStreamResource
import spock.lang.Narrative
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.charset.Charset
import java.nio.charset.UnsupportedCharsetException

/**
 * Test FixedByteLengthBufferedReaderFactory
 *
 * @since 5.0.0
 */

@Narrative("""
Create BufferedReader for reading simple text files with no line endings and tokens separated by a fixed byte length.
BufferedReader splits based on a specified fixed byte length.
""")
class FixedByteLengthBufferedReaderFactorySpec extends Specification {

    def content = "0123456789abcde"
    def encoding = "MS932"
    def charset = Charset.forName(encoding)
    def byteLength = 5

    def "Constructor param 'byteLength' cannot be zero."() {
        when:
        new FixedByteLengthBufferedReaderFactory(0)
        then:
        def ex = thrown(IllegalArgumentException)
        ex.getMessage() == "byteLength must be higher than zero. [byteLength:0]"
    }

    def "Constructor sets the parameter to the field."() {
        when:
        def factory = new FixedByteLengthBufferedReaderFactory(10)

        then:
        factory.@byteLength == 10
    }

    def "Create method arguments 'resource' cannot be null."() {
        setup:
        def factory = new FixedByteLengthBufferedReaderFactory(byteLength)

        when:
        factory.create(null, encoding)
        then:
        def ex = thrown(IllegalArgumentException)
        ex.getMessage() == "resource must be set."
    }

    def "Create method arguments 'encoding' cannot be null."() {
        setup:
        def resource = new ByteArrayResource(content.getBytes(charset))
        def factory = new FixedByteLengthBufferedReaderFactory(byteLength)

        when:
        factory.create(resource, null)

        then:
        // Exception occurs in Charset#lookup()
        def ex = thrown(IllegalArgumentException)
        ex.getMessage() == "Null charset name"
    }

    def "Create method arguments 'encoding' is must be supported."() {
        setup:
        def resource = new ByteArrayResource(content.getBytes(charset))
        def factory = new FixedByteLengthBufferedReaderFactory(byteLength)
        def encoding = "unsupportedCharsetName"

        when:
        factory.create(resource, encoding)

        then:
        def ex = thrown(UnsupportedCharsetException)
        ex.getMessage() == "unsupportedCharsetName"
    }

    def "Create method get an instance of FixedByteLengthBufferedReader."() {
        setup:
        def resource = new ByteArrayResource(content.getBytes(charset))
        def factory = new FixedByteLengthBufferedReaderFactory(byteLength)

        when:
        def reader = factory.create(resource, encoding)

        then:
        reader.getClass().getName() == "org.terasoluna.batch.item.file.FixedByteLengthBufferedReaderFactory\$FixedByteLengthBufferedReader"
        reader.@in.getClass() == resource.getInputStream().getClass()
        new String(reader.@in.@buf, charset) == content
        reader.@charset == charset
        reader.@byteLength == byteLength
    }

    @Unroll
    def "Read line as (#line1), (#line2) and (#line3) from content(#content) when specified byteLength(#byteLength) and encoding(#encoding)"() {
        setup:
        def charset = Charset.forName(encoding)
        def resource = new ByteArrayResource(content.getBytes(charset))
        def factory = new FixedByteLengthBufferedReaderFactory(byteLength)
        def reader = factory.create(resource, encoding)

        expect:
        reader.readLine() == line1
        reader.readLine() == line2
        reader.readLine() == line3

        where:
        content                     | byteLength | encoding || line1            | line2         | line3
        // encoding variation
        "0123456789abcdefghij"      | 10         | "UTF-8"  || "0123456789"     | "abcdefghij"  | null
        "0123456789abcdefghij"      | 10         | "ASCII"  || "0123456789"     | "abcdefghij"  | null
        "0123456789abcdefghij"      | 10         | "MS932"  || "0123456789"     | "abcdefghij"  | null
        "0123456789abcdefghij"      | 10         | "EUC-JP" || "0123456789"     | "abcdefghij"  | null
        // contents variation
        "0123  6789  cdefgh  "      | 10         | "UTF-8"  || "0123  6789"     | "  cdefgh  "  | null // no-trim
        "0\r\n34567\r\na\".,e/@<>j" | 10         | "UTF-8"  || "0\r\n34567\r\n" | "a\".,e/@<>j" | null // control, symbol
        "  1あ ２い 三う"           | 10         | "UTF-8"  || "  1あ ２"       | "い 三う"     | null // double-byte
        " 1あ𠮷  ２い  1"           | 10         | "UTF-8"  || " 1あ𠮷 "        | " ２い  1"    | null // surrogate pair
    }

    def "readByteLength is must be equal to byteLength."() {
        setup:
        def resource = new ByteArrayResource(content.getBytes(charset))
        def factory = new FixedByteLengthBufferedReaderFactory(20)
        def reader = factory.create(resource, encoding)

        when:
        reader.readLine()

        then:
        def ex = thrown(IncorrectLineLengthException)
        ex.getMessage() == "readByteLength is less than byteLength. [readByteLength:15][byteLength:20]"
    }

    def "Call unsupported operations, UnsupportedOperationException is thrown."() {
        setup:
        def resource = new ByteArrayResource(content.getBytes(charset))
        def factory = new FixedByteLengthBufferedReaderFactory(byteLength)
        def reader = factory.create(resource, encoding)
        def byteArray = new char[5]

        when:
        reader.read()

        then:
        def ex1 = thrown(UnsupportedOperationException)
        ex1.getMessage() == "read not supported."

        when:
        reader.read(byteArray, 0, 5)

        then:
        def ex2 = thrown(UnsupportedOperationException)
        ex2.getMessage() == "read not supported."

        when:
        reader.skip(5)

        then:
        def ex3 = thrown(UnsupportedOperationException)
        ex3.getMessage() == "skip not supported."

        when:
        reader.ready()

        then:
        def ex4 = thrown(UnsupportedOperationException)
        ex4.getMessage() == "ready not supported."

        when:
        reader.markSupported()

        then:
        def ex5 = thrown(UnsupportedOperationException)
        ex5.getMessage() == "markSupported not supported."

        when:
        reader.mark(5)

        then:
        def ex6 = thrown(UnsupportedOperationException)
        ex6.getMessage() == "mark not supported."

        when:
        reader.reset()

        then:
        def ex7 = thrown(UnsupportedOperationException)
        ex7.getMessage() == "reset not supported."

        when:
        reader.lines()

        then:
        def ex8 = thrown(UnsupportedOperationException)
        ex8.getMessage() == "lines not supported."
    }

    def "Call close method, exception does not occur."() {
        setup:
        def resource = new ByteArrayResource(content.getBytes(charset))
        def factory = new FixedByteLengthBufferedReaderFactory(byteLength)
        def reader = factory.create(resource, encoding)

        when:
        reader.close()

        then:
        noExceptionThrown()
    }

    def "Close method does not do nothing if IOException occur in close method of superclass"() {
        setup:
        def resource = new ByteArrayResource(content.getBytes(charset))
        def factory = new FixedByteLengthBufferedReaderFactory(byteLength)
        def reader = factory.create(resource, encoding)

        def mockReader = Mock(Reader)
        mockReader.close() >> { throw new IOException() }

        def field = reader.class.superclass.getDeclaredField("in")
        field.setAccessible(true)
        field.set(reader, mockReader)

        when:
        reader.close()

        then:
        noExceptionThrown()
    }

    def "Close method does not do nothing if IOException occur in close method of input stream."() {
        setup:
        def factory = new FixedByteLengthBufferedReaderFactory(byteLength)

        def byteArrayInputStream = Mock(ByteArrayInputStream)
        byteArrayInputStream.close() >> { throw new IOException() }
        def inputStreamResource = new InputStreamResource(byteArrayInputStream)

        def reader = factory.create(inputStreamResource, encoding)

        when:
        reader.close()

        then:
        noExceptionThrown()
    }
}