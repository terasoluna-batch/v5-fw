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
package org.terasoluna.batch.async.db.model;

/**
 * Represent Polling Status.
 * <p>
 * Represent the simple progress of the job request and its job.
 * </p>
 * 
 * @since 5.0.0
 */
public enum PollingStatus {
    /**
     * Initial status.
     * <p>
     * Job request is not picked up, and no job is executed.
     * </p>
     */
    INIT,

    /**
     * Job request is picked up, and job is scheduled and executing.
     */
    POLLED,

    /**
     * Job request is picked up, and job is executed.
     */
    EXECUTED
}
