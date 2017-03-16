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
package org.terasoluna.batch.async.db.model

import spock.lang.Specification

import java.sql.Timestamp

/**
 * Test BatchJobRequest
 *
 * @since 5.0.0
 */
class BatchJobRequestSpec extends Specification {

    def "Setting the instance generation and initial value"() {

        when:
        def request = new BatchJobRequest()

        then:
        request.jobSeqId == 0L
        request.jobName == null
        request.jobParameter == null
        request.pollingStatus == null
        request.jobExecutionId == null
        request.createDate == null
        request.updateDate == null
    }

    def "Access to the property"() {
        setup:
        def jobSeqId = 123L
        def jobName = "Job0001"
        def jobParameter = "Param1=2011-01-02 Param2=222"
        def pollingStatus = PollingStatus.INIT
        def jobExecutionId = 456L
        def createDate = new Timestamp(System.currentTimeMillis())
        def updateDate = new Timestamp(System.currentTimeMillis() + 1L)
        def request = new BatchJobRequest()

        when:
        request.setJobSeqId(jobSeqId)
        request.setJobName(jobName)
        request.setJobParameter(jobParameter)
        request.setJobExecutionId(jobExecutionId)
        request.setPollingStatus(pollingStatus)
        request.setCreateDate(createDate)
        request.setUpdateDate(updateDate)

        then:
        request.getJobSeqId() == jobSeqId
        request.getJobName() == jobName
        request.getJobParameter() == jobParameter
        request.getJobExecutionId() == jobExecutionId
        request.getPollingStatus() == pollingStatus
        request.getCreateDate() == createDate
        request.getUpdateDate() == updateDate
    }
}
