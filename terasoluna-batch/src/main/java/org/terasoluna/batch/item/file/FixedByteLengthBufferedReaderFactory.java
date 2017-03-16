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
package org.terasoluna.batch.item.file;

import org.springframework.batch.item.file.BufferedReaderFactory;
import org.springframework.batch.item.file.transform.IncorrectLineLengthException;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.stream.Stream;

/**
 * A {@link BufferedReaderFactory} useful for reading simple text files with no line endings and tokens separated by a fixed
 * byte length.
 * <p>
 * The reader splits a stream up across fixed line endings (rather than the usual convention based on plain text). This class is
 * intended only to be used from FlatFileItemReader.
 * </p>
 * <p>
 * The line endings are not discarded, line endings are also included in the fixed byte length. Therefore, please be careful
 * when setting a fixed byte length.
 * </p>
 *
 * @since 5.0.0
 */
public class FixedByteLengthBufferedReaderFactory implements BufferedReaderFactory {

    /**
     * The byte length of a line.
     */
    private final int byteLength;

    /**
     * Create a new instance with the specified parameters.
     *
     * @param byteLength The byte length of a line.
     * @throws IllegalArgumentException if {@code byteLength} is not higher than zero.
     */
    public FixedByteLengthBufferedReaderFactory(int byteLength) {
        Assert.isTrue(byteLength > 0, "byteLength must be higher than zero. [byteLength:" + byteLength + "]");
        this.byteLength = byteLength;
    }

    /**
     * {@inheritDoc}
     *
     * @return extended {@link BufferedReader} that reading String items based on a specified fixed byte length.
     */
    @Override
    public BufferedReader create(Resource resource, String encoding) throws IOException {
        Assert.notNull(resource, "resource must be set.");
        return new FixedByteLengthBufferedReader(resource.getInputStream(), encoding, byteLength);
    }

    /**
     * BufferedReader extension that splits lines based on a specified fixed byte length.
     * <p>
     * Be careful because this class is thread unsafe. This class is intended only to be used from FlatFileItemReader.
     * </p>
     *
     * @since 5.0.0
     */
    private final class FixedByteLengthBufferedReader extends BufferedReader {

        /**
         * Input stream reading the input file.
         */
        private final InputStream in;

        /**
         * Charset for input file.
         */
        private final Charset charset;

        /**
         * The byte length of a line.
         */
        private final int byteLength;

        /**
         * Create a new instance with the specified parameters.
         * <p>
         * In addition, it set the dummy reader instance according to the super class's constructor.
         * </p>
         *
         * @param in InputStream obtained from resource.
         * @param encoding the encoding required for converting binary data to String.
         * @param byteLength the byte length of a line.
         */
        private FixedByteLengthBufferedReader(InputStream in, String encoding, int byteLength) {
            super(new StringReader("dummy"));
            this.in = in;
            this.charset = Charset.forName(encoding);
            this.byteLength = byteLength;
        }

        /**
         * Reads the specified fixed byte length as a line from this input stream.
         *
         * @return a string converted from fixed bytes, or null if the end of the stream has been reached
         * @throws IncorrectLineLengthException if line length is less than the fixed byte length.
         * @throws IOException if an I/O error occurs.
         */
        @Override
        public String readLine() throws IOException {

            byte[] line = new byte[byteLength];

            int readByteLength = in.read(line);

            if (readByteLength == byteLength) {

                return new String(line, charset);

            } else if (readByteLength == -1) {

                return null;

            } else {

                throw new IncorrectLineLengthException("readByteLength is less than byteLength. [readByteLength:"
                        + readByteLength + "]" + "[byteLength:" + byteLength + "]", byteLength, readByteLength, new String(line, 0, readByteLength, charset));

            }

        }

        /**
         * This operation is not supported.
         *
         * @throws UnsupportedOperationException always throw.
         */
        @Override
        public int read() {
            throw new UnsupportedOperationException("read not supported.");
        }

        /**
         * This operation is not supported.
         *
         * @param cbuf omitted.
         * @param off omitted.
         * @param len omitted.
         * @throws UnsupportedOperationException always throw.
         */
        @Override
        public int read(char[] cbuf, int off, int len) {
            throw new UnsupportedOperationException("read not supported.");
        }

        /**
         * This operation is not supported.
         *
         * @param n omitted.
         * @throws UnsupportedOperationException always throw.
         */
        @Override
        public long skip(long n) {
            throw new UnsupportedOperationException("skip not supported.");
        }

        /**
         * This operation is not supported.
         *
         * @throws UnsupportedOperationException always throw.
         */
        @Override
        public boolean ready() {
            throw new UnsupportedOperationException("ready not supported.");
        }

        /**
         * This operation is not supported.
         *
         * @throws UnsupportedOperationException always throw.
         */
        @Override
        public boolean markSupported() {
            throw new UnsupportedOperationException("markSupported not supported.");
        }

        /**
         * This operation is not supported.
         *
         * @param readAheadLimit omitted.
         * @throws UnsupportedOperationException always throw.
         */
        @Override
        public void mark(int readAheadLimit) {
            throw new UnsupportedOperationException("mark not supported.");
        }

        /**
         * This operation is not supported.
         *
         * @throws UnsupportedOperationException always throw.
         */
        @Override
        public void reset() throws IOException {
            throw new UnsupportedOperationException("reset not supported.");
        }

        /**
         * This operation is not supported.
         *
         * @throws UnsupportedOperationException always throw.
         */
        @Override
        public Stream<String> lines() {
            throw new UnsupportedOperationException("lines not supported.");
        }

        /**
         * Closes this input stream, reader and releases any system resources associated with these.
         */
        @Override
        public void close() {
            try {
                super.close();
            } catch (IOException ioe) {
                // do nothing
            }
            try {
                in.close();
            } catch (IOException ioe) {
                // do nothing
            }
        }
    }

}
