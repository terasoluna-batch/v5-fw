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
package org.terasoluna.batch.async.db.repository;

import org.apache.ibatis.annotations.Param;
import org.terasoluna.batch.async.db.model.BatchJobRequest;
import org.terasoluna.batch.async.db.model.PollingStatus;

import java.util.List;
import java.util.Map;

/**
 * Batch Job Request Table Mapper.
 * <p>
 * Batch Job Request Table access interface with MyBatis. Bean is created by {@code <mybatis:scan>} in xml application context
 * file.
 * </p>
 * 
 * @since 5.0.0
 */
public interface BatchJobRequestMapper {

    /**
     * Retrieve Batch Job Request Table by any parameters.
     * 
     * @param parameters Query parameters.
     * @return List of job requests that match the conditions. When data not found, return empty list.
     */
    List<BatchJobRequest> find(Map<String, Object> parameters);

    /**
     * Update Batch Job Request Table polling status.
     *
     * @param batchJobRequest Updated data.
     * @param pollingStatus Update condition of polling status.
     * @return Updated record number.
     */
    int updateStatus(@Param("batchJobRequest") BatchJobRequest batchJobRequest,
            @Param("pollingStatus") PollingStatus pollingStatus);

}
