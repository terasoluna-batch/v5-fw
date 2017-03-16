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

import java.sql.Timestamp;

/**
 * Model of Batch Job Request Table.
 * 
 * @since 5.0.0
 */
public class BatchJobRequest {

    /**
     * Job sequence id.
     */
    private long jobSeqId;

    /**
     * Job name.
     */
    private String jobName;

    /**
     * Job parameter.
     */
    private String jobParameter;

    /**
     * Polling status.
     */
    private PollingStatus pollingStatus;

    /**
     * Job Execution Id.
     */
    private Long jobExecutionId;

    /**
     * Timestamp of creating job request.
     */
    private Timestamp createDate;

    /**
     * Timestamp of updating job request.
     */
    private Timestamp updateDate;

    /**
     * Unique sequence number of the job request.
     *
     * @return The current unique sequence number.
     */
    public long getJobSeqId() {
        return jobSeqId;
    }

    /**
     * Unique sequence number of the job request.
     * 
     * @param jobSeqId New unique sequence number.
     */
    public void setJobSeqId(long jobSeqId) {
        this.jobSeqId = jobSeqId;
    }

    /**
     * Job name.
     * 
     * @return The current job name
     */
    public String getJobName() {
        return jobName;
    }

    /**
     * Job name.
     * 
     * @param jobName New job name
     */
    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    /**
     * Job parameters.
     * <p>
     * A parameter style is {@code <key>=<value>}. Delimiter of the parameters to each other is blank.
     * </p>
     * 
     * @return The current job parameters.
     */
    public String getJobParameter() {
        return jobParameter;
    }

    /**
     * Job parameters.
     * <p>
     * A parameter style is {@code <key>=<value>}. Delimiter of the parameters to each other is blank.
     * </p>
     * 
     * @param jobParameter New job parameters.
     */
    public void setJobParameter(String jobParameter) {
        this.jobParameter = jobParameter;
    }

    /**
     * Polling Status.
     * 
     * @return The current job parameters.
     * @see PollingStatus
     */
    public PollingStatus getPollingStatus() {
        return pollingStatus;
    }

    /**
     * Polling Status.
     * 
     * @param pollingStatus New polling status.
     * @see PollingStatus
     */
    public void setPollingStatus(PollingStatus pollingStatus) {
        this.pollingStatus = pollingStatus;
    }

    /**
     * Job execution id.
     * <p>
     * After job finished, job execution id is set to set..
     * </p>
     * 
     * @return The current job execution id.
     */
    public Long getJobExecutionId() {
        return jobExecutionId;
    }

    /**
     * Job execution id.
     * <p>
     * After job finished, job execution id is set to set..
     * </p>
     * 
     * @param jobExecutionId New job execution id.
     */
    public void setJobExecutionId(Long jobExecutionId) {
        this.jobExecutionId = jobExecutionId;
    }

    /**
     * Time stamp of when a job request is created.
     * 
     * @return The current timestamp of created.
     */
    public Timestamp getCreateDate() {
        return createDate;
    }

    /**
     * Time stamp of when a job request is created.
     * 
     * @param createDate New timestamp of created.
     */
    public void setCreateDate(Timestamp createDate) {
        this.createDate = createDate;
    }

    /**
     * Time stamp of when a job request is updated.
     * 
     * @return The current timestamp of updated
     */
    public Timestamp getUpdateDate() {
        return updateDate;
    }

    /**
     * Time stamp of when a job request is updated.
     * 
     * @param updateDate New timestamp of updated.
     */
    public void setUpdateDate(Timestamp updateDate) {
        this.updateDate = updateDate;
    }

    /**
     * Returns a string representation of this {@code BatchJobRequest}.
     *
     * @return a string representation of this {@code BatchJobRequest}.
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BatchJobRequest{");
        sb.append("jobSeqId=").append(jobSeqId);
        sb.append(", jobName='").append(jobName).append('\'');
        sb.append(", jobParameter='").append(jobParameter).append('\'');
        sb.append(", pollingStatus=").append(pollingStatus);
        sb.append(", jobExecutionId=").append(jobExecutionId);
        sb.append(", createDate=").append(createDate);
        sb.append(", updateDate=").append(updateDate);
        sb.append('}');
        return sb.toString();
    }
}
