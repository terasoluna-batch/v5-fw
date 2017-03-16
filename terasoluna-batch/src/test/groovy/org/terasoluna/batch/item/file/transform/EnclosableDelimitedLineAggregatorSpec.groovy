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

import org.springframework.context.support.ClassPathXmlApplicationContext
import org.springframework.util.ClassUtils
import spock.lang.Narrative
import spock.lang.Specification
import spock.lang.Unroll
/**
 * UnitTest code for EnclosableDelimitedLineAggregator
 *
 * @since 5.0.0
 */
@Narrative("""
To Test the possibility to convert regular delimited string line from array of fields.
And check the error when parameter set the specific illegal value.
""")
class EnclosableDelimitedLineAggregatorSpec extends Specification {

    def "setAllEnclosing-method can be changed."() {
        setup:
        def edla = new EnclosableDelimitedLineAggregator()

        when:
        edla.setAllEnclosing(true)

        then:
        edla.allEnclosing == true
    }

    def "setEnclosure-method can be set any value."() {
        setup:
        def edla = new EnclosableDelimitedLineAggregator()

        when:
        edla.setEnclosure('|' as char)

        then:
        edla.enclosure == '|'
        edla.escapedEnclosure == '||'
    }

    def "setDelimiter-method can be set any value."() {
        setup:
        def edla = new EnclosableDelimitedLineAggregator()

        when:
        edla.setDelimiter('|' as char)

        then:
        edla.delimiter == '|'
    }

    def "afterPropertiesSet-method throw exception when delimiter and enclosure are same."() {
        setup:
        def edla = new EnclosableDelimitedLineAggregator()
        edla.setDelimiter('+' as char)
        edla.setEnclosure('+' as char)

        when:
        edla.afterPropertiesSet()

        then:
        def ex = thrown(IllegalStateException)
        ex.getMessage() == "the delimiter and enclosure must be different. [value:+]"
    }

    def "afterPropertiesSet-method ok when delimiter and enclosure are not same."() {
        setup:
        def edla = new EnclosableDelimitedLineAggregator()

        when:
        edla.setDelimiter('|' as char)
        edla.setEnclosure('+' as char)
        edla.afterPropertiesSet()

        then:
        noExceptionThrown();
    }

    @Unroll
    def "when param(#field) and format(#format), hasTargetChar-method return(#result)."() {
        setup:
        def edla = new EnclosableDelimitedLineAggregator()

        expect:
        edla.setAllEnclosing(format)
        edla.hasTargetChar(field) == result

        where:
        format | field        || result
        // not target field and enclosing-format(All and Partly) check
        true   | 'aa'         || true
        false  | 'aa'         || false
        // a field contains delimiter, enclosure, carriage-return, line-feed.
        false  | 'a,a'        || true
        false  | 'a"a'        || true
        false  | 'a\ra'       || true
        false  | 'a\na'       || true
        // target character position.
        false  | ',aa'        || true
        false  | 'aa,'        || true
        // target field escaped.
        false  | 'a\\ra'      || false
        // contains some targets.
        false  | ',a"a\ra\na' || true
    }

    @Unroll
    def "encloseAndEscape-method param(#field) return(#result)."() {
        def edla = new EnclosableDelimitedLineAggregator()

        expect:
        edla.encloseAndEscape(field) == result

        where:
        field   || result
        'aaa'   || '"aaa"'
        'a"aa'  || '"a""aa"'
        'a""aa' || '"a""""aa"'
    }

    @Unroll
    def "doAggregate-method convert from(#fields) to(#result)."() {
        setup:
        def edla = new EnclosableDelimitedLineAggregator()

        expect:
        edla.doAggregate(fields) == result

        where:
        fields                                               || result
        ['aa', 'bb', 'cc', 'dd', 'ee'] as Object[]           || 'aa,bb,cc,dd,ee'
        ['a,a', 'b"b', 'c\nc', 'd\rd', 'e\r\ne'] as Object[] || '"a,a","b""b","c\nc","d\rd","e\r\ne"'
    }

    @Unroll
    def "doAggregate-method convert from(#fields) to(#result) with changed delimiter(#delimiter)."() {
        setup:
        def edla = new EnclosableDelimitedLineAggregator()

        expect:
        edla.setDelimiter(delimiter as char)
        edla.doAggregate(fields) == result

        where:
        delimiter | fields                    || result
        '|'       | ['a,a', 'bb'] as Object[] || 'a,a|bb'
        '|'       | ['a|a', 'bb'] as Object[] || '"a|a"|bb'
    }

    @Unroll
    def "doAggregate-method convert from(#fields) to(#result) with changed enclosure(#enclosure)."() {
        setup:
        def edla = new EnclosableDelimitedLineAggregator()

        expect:
        edla.setEnclosure(enclosure as char)
        edla.doAggregate(fields) == result

        where:
        enclosure | fields || result
        '+'       | ['a,a'  , 'bb'] as Object[]  || '+a,a+,bb'
        '+'       | ['a+a'  , 'bb'] as Object[]  || '+a++a+,bb'
    }

    @Unroll
    def "doAggregate-method convert from(#fields) to(#result) with changed format(#format)."() {
        setup:
        def edla = new EnclosableDelimitedLineAggregator()

        expect:
        edla.setAllEnclosing(format as boolean)
        edla.doAggregate(fields) == result

        where:
        format | fields                    || result
        true   | ['aa', 'bb'] as Object[]  || '"aa","bb"'
        false  | ['aa', 'bb'] as Object[]  || 'aa,bb'
        false  | ['aa', 'b,b'] as Object[] || 'aa,"b,b"'
    }

    @Unroll
    def "doAggregate-method is based on toString-result, when set delimiter(#delimiter), enclosure(#enclosure), param(#fields), it return(#result)."() {
        def edla = new EnclosableDelimitedLineAggregator()

        expect:
        edla.setDelimiter(delimiter)
        edla.setEnclosure(enclosure)
        edla.doAggregate(fields) == result

        where:
        delimiter   | enclosure   | fields                   || result
        '.' as char | '"' as char | [999, 99.99] as Object[] || '999."99.99"'
        ',' as char | '.' as char | [999, 99.99] as Object[] || '999,.99..99.'
    }

    def "At using xml bean definition, set simple char by direct value."() {

        setup:
        def configLocation = ClassUtils.addResourcePathToPackagePath(EnclosableDelimitedLineAggregatorSpec.class,
                "colon-def-enclosable-aggregator.xml")
        def context = new ClassPathXmlApplicationContext(configLocation)
        def lineAggregator = context.getBean(EnclosableDelimitedLineAggregator.class)

        expect:
        lineAggregator.doAggregate(['aaa', 'bbb'] as Object[]) == 'aaa:bbb'

        cleanup:
        if (context != null) {
            context.close()
        }

    }

    def "At using xml bean definition, set control char by character reference."() {

        setup:
        def configLocation = ClassUtils.addResourcePathToPackagePath(EnclosableDelimitedLineAggregatorSpec.class,
                "tsv-def-enclosable-aggregator.xml")
        def context = new ClassPathXmlApplicationContext(configLocation)
        def lineAggregator = context.getBean(EnclosableDelimitedLineAggregator.class)

        expect:
        lineAggregator.doAggregate(['aaa', 'bbb'] as Object[]) == 'aaa\tbbb'

        cleanup:
        if (context != null) {
            context.close()
        }

    }
}