/*
 * Copyright (C) 2025 NTT DATA Corporation
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
package org.terasoluna.batch.async.db;

/**
 * Class for terminating the JVM.
 *
 * This class is intended to make components that directly call {@link System#exit(int)}
 * testable, by preventing the entire JVM from being terminated when {@code System.exit}
 * is invoked during unit tests.
 *
 * @since 5.8.0
 */
public class SystemExiter {

    /**
     * Terminates the currently running Java Virtual Machine.
     * @param status the exit status code
     */
    protected void exit(int status) {
        System.exit(status);
    }
}
