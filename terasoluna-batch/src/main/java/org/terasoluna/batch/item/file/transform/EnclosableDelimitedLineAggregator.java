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

import org.springframework.batch.item.file.transform.ExtractorLineAggregator;
import org.springframework.beans.factory.InitializingBean;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * A {@link org.springframework.batch.item.file.transform.LineAggregator} implementation that converts an object into a
 * delimited single string.
 * <p>
 * This class supports enclosure and delimiter for like CSV file.
 * {@link org.springframework.batch.item.file.transform.DelimitedLineAggregator} supports only delimiter, but enclosure does
 * not. The purpose of this class is to support specification as follows (which based on RFC 4180).
 * </p>
 * <ul>
 * <li>Field containing line breaks, enclosure, and delimiter is enclosed in enclosure.</li>
 * <li>Enclosure appearing inside a field, it is escaped by preceding it with another enclosure.</li>
 * </ul>
 * <p>
 * The delimiter and enclosure can be changed to any character. Default delimiter is comma, and default enclosure is
 * double-quote. This class support two enclosing-format as follows.
 * </p>
 * <ul>
 * <li>Partly-Enclosing (default) : Only if a field contains line breaks, enclosure character, and delimiter, it will be
 * enclosed. Otherwise, it will not be enclosed.</li>
 * <li>All-Enclosing : All fields will be enclosed.</li>
 * </ul>
 * <p>
 * This class cannot be set to the same character to the enclosure and delimiter.
 * </p>
 * <p>
 * Examples
 * </p>
 * <ul>
 * <li>Field enclosed by double-quote : "aa,aa","bb\r\nbb",cccc</li>
 * <li>Enclosure-character is escaped : aaaa,bbbb,"cc""cc"</li>
 * <li>Partly-Enclosing : "aa,aa",bbbb,cccc</li>
 * <li>All-Enclosing : "aa,aa","bbbb","cccc"</li>
 * </ul>
 *
 * @param <T> Type of value to be converted
 * @since 5.0.0
 */
public class EnclosableDelimitedLineAggregator<T> extends ExtractorLineAggregator<T> implements InitializingBean {

    /**
     * Enclosing-format.
     */
    protected boolean allEnclosing = false;

    /**
     * Enclosure character.
     */
    protected String enclosure = "\"";

    /**
     * Escaped enclosure character.
     */
    protected String escapedEnclosure = enclosure + enclosure;

    /**
     * Delimiter character.
     */
    protected String delimiter = ",";

    /**
     * Public setter for the variable {@code allEnclosing}.
     * <ul>
     * <li>true : All-Enclosing</li>
     * <li>false : Partly-Enclosing</li>
     * </ul>
     *
     * @param allEnclosing Enclosing format type. Default value is false.
     */
    public void setAllEnclosing(boolean allEnclosing) {
        this.allEnclosing = allEnclosing;
    }

    /**
     * Public setter for the variable {@code enclosure}.
     * <p>
     * {@code escapedEnclosure} is replaced double {@code enclosure} character.
     * </p>
     *
     * @param enclosure Enclosure character. Default value is double-quote.
     */
    public void setEnclosure(char enclosure) {
        String e = String.valueOf(enclosure);
        this.enclosure = e;
        this.escapedEnclosure = e + e;
    }

    /**
     * Public setter for the variable {@code delimiter}.
     *
     * @param delimiter Delimiter. Default value is comma.
     */
    public void setDelimiter(char delimiter) {
        this.delimiter = String.valueOf(delimiter);
    }

    /**
     * Check enclosure and delimiter are not same.
     *
     * @throws IllegalStateException If enclosure and delimiter are same.
     */
    @Override
    public void afterPropertiesSet() {
        if (enclosure.equals(delimiter)) {
            throw new IllegalStateException("the delimiter and enclosure must be different. [value:" + enclosure + "]");
        }
    }

    /**
     * Aggregate provided fields into single string.
     *
     * @param fields An array of the fields that must be aggregated.
     * @return Delimited and escaped line string.
     */
    @Override
    protected String doAggregate(Object[] fields) {

        return Arrays.stream(fields).map(Object::toString)
                .map(field -> hasTargetChar(field) ? encloseAndEscape(field) : field)
                .collect(Collectors.joining(delimiter));
    }

    /**
     * Check the {@code field} contains target character.
     *
     * @param field An element of an array.
     * @return true : A field contains delimiter, enclosure, carriage-return, line-feed.
     */
    private boolean hasTargetChar(String field) {
        return allEnclosing || (field.contains(delimiter)) || (field.contains(enclosure)) || containsCrlf(field);
    }

    /**
     * Check the {@code field} contains CR or LF especially.
     * 
     * @param field An element of an array.
     * @return true if field contains CR or LF.
     */
    private boolean containsCrlf(String field) {
        return (field.contains("\r")) || (field.contains("\n"));
    }

    /**
     * Do enclose and escape.
     *
     * @param field A target string.
     * @return A enclosed and escaped string.
     */
    private String encloseAndEscape(String field) {
        return enclosure + field.replace(enclosure, escapedEnclosure) + enclosure;
    }
}
