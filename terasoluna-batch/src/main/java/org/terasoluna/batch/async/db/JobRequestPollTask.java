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
package org.terasoluna.batch.async.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.support.AutomaticJobRegistrar;
import org.springframework.batch.core.launch.JobInstanceAlreadyExistsException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.util.Assert;
import org.terasoluna.batch.async.db.model.BatchJobRequest;
import org.terasoluna.batch.async.db.model.PollingStatus;
import org.terasoluna.batch.async.db.repository.BatchJobRequestRepository;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Batch job request polling task.
 * <p>
 * It is a task to poll the batch job request to start the job. Polling period is scheduled at a fixed delay. The maximum number
 * of job requests to get in polling is the same as the number of concurrent jobs. Jobs in this task is based on the premise
 * that it is executed synchronously.
 * </p>
 * <p>
 * Concurrent number of jobs has to be set in the pool size of task executor injected into the task. The property of Concurrent
 * number are as follows
 * </p>
 * <ul>
 * <li>async-batch-daemon.job-concurrency-num: The number of concurrent jobs. default values is 3.</li>
 * </ul>
 * <p>
 * This task if the daemon is stopped, to end waiting a certain period of time until the job is completed. The property of await
 * termination time are as follows
 * </p>
 * <ul>
 * <li>async-batch-daemon.job-await-termination-seconds: The number of seconds until the job is terminated. default values is
 * 600 sec.</li>
 * </ul>
 * 
 * @since 5.0.0
 */

public class JobRequestPollTask implements InitializingBean, DisposableBean {

    /**
     * logger.
     */
    private static final Logger logger = LoggerFactory.getLogger(JobRequestPollTask.class);

    /**
     * Batch Job Request Table Mapper.
     */
    private final BatchJobRequestRepository batchJobRequestRepository;

    /**
     * Transaction manager.
     */
    private final PlatformTransactionManager transactionManager;

    /**
     * Task executor for handling threads to perform the job.
     */
    private final ThreadPoolTaskExecutor daemonTaskExecutor;

    /**
     * Job operator for the lunching requested job.
     */
    private final JobOperator jobOperator;

    /**
     * AutomaticJobRegistrar for waiting completion of initializing jobRegistry.
     */
    private final AutomaticJobRegistrar automaticJobRegistrar;

    /**
     * Get up the number of job requests. It is equal to the number of concurrent jobs.
     */
    @Value("${async-batch-daemon.job-concurrency-num:3}")
    private int pollingRowLimit;

    /**
     * Time to wait until the job is finished when the daemon stop.
     */
    @Value("${async-batch-daemon.job-await-termination-seconds:600}")
    private int awaitTerminationSeconds;

    /**
     * Flags daemon is the end state.
     */
    private volatile boolean shutdownCalled = false;

    /**
     * Optional polling query parameters.
     */
    private Map<String, Object> optionalPollingQueryParams;

    /**
     * Polling query parameters.
     */
    private Map<String, Object> pollingQueryParams = new HashMap<>();

    /**
     * Output message flag of executing poll method.
     */
    private boolean enablePollingLog = true;

    /**
     * Clock for getting timestamp
     */
    private Clock clock = Clock.systemDefaultZone();

    /**
     * Create JobRequestPollTask instance.
     * 
     * @param batchJobRequestRepository {@link BatchJobRequestRepository}.
     * @param transactionManager Transaction manager.
     * @param daemonTaskExecutor Thread poll task executor for concurrently execute job.
     * @param automaticJobRegistrar AutomaticJobRegistrar for waiting completion of initializing jobRegistry.
     * @param jobOperator Job operator for launch job.
     */
    public JobRequestPollTask(BatchJobRequestRepository batchJobRequestRepository,
            PlatformTransactionManager transactionManager, ThreadPoolTaskExecutor daemonTaskExecutor,
            JobOperator jobOperator, AutomaticJobRegistrar automaticJobRegistrar) {

        Assert.notNull(batchJobRequestRepository, "batchJobRequestRepository must be not null.");
        Assert.notNull(transactionManager, "transactionManager must be not null.");
        Assert.notNull(daemonTaskExecutor, "daemonTaskExecutor must be not null.");
        Assert.notNull(jobOperator, "jobOperator must be not null.");
        Assert.notNull(automaticJobRegistrar, "automaticJobRegistrar must be not null.");

        this.batchJobRequestRepository = batchJobRequestRepository;
        this.transactionManager = transactionManager;
        this.daemonTaskExecutor = daemonTaskExecutor;
        this.jobOperator = jobOperator;
        this.automaticJobRegistrar = automaticJobRegistrar;

    }

    /**
     * Polling processing.
     * <p>
     * It is a endpoint to poll the batch job request to start the job. Polling period is scheduled at a fixed delay. This
     * period is set to application property file.
     * </p>
     * <p>
     * The key of property are as follows
     * </p>
     * <ul>
     * <li>async-batch-daemon.polling-interval: polling interval value. default value is 10000 msec.</li>
     * <li>async-batch-daemon.polling-initial-delay: initial delay value. default value is 1000 msec.</li>
     * </ul>
     * <p>
     * async-batch-daemon.polling-initial-delay is The delay is the purpose of the waiting time to start polling after the end
     * daemon start check processing.
     * </p>
     */
    @Scheduled(fixedDelayString = "${async-batch-daemon.polling-interval:10000}", initialDelayString = "${async-batch-daemon.polling-initial-delay:1000}")
    public void poll() {
        if (enablePollingLog) {
            logger.info("Polling processing.");
        }

        if (shutdownCalled) {
            return;
        }

        if (!automaticJobRegistrar.isRunning()) {
            logger.info("Put off polling, because jobRegistry is not on running status.");
            return;
        }

        DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setName("retrieveInitStatusJobRequest");
        definition.setReadOnly(true);
        TransactionStatus status = transactionManager.getTransaction(definition);

        try {
            List<BatchJobRequest> requests = batchJobRequestRepository.find(pollingQueryParams);
            for (final BatchJobRequest request : requests) {
                try {
                    String jobParams = request.getJobParameter();
                    if(jobParams == null){
                        request.setJobParameter("");
                    }else {
                        if (jobParams.contains(",")) {
                            request.setJobParameter(jobParams.replace(",", " "));
                        }
                    }
                    daemonTaskExecutor.execute(() -> executeJob(request));
                } catch (TaskRejectedException e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "Concurrency number of executing job is over, and skip this and after requests. [{}]",
                                request);
                    }
                    break;
                }
            }
        } finally {
            transactionManager.commit(status);
        }
    }

    /**
     * Execute requested job.
     * 
     * @param batchJobRequest Batch job request.
     */
    void executeJob(BatchJobRequest batchJobRequest) {
        if (updateStatusPolled(batchJobRequest)) {
            try {
                Long jobExecutionId = jobOperator
                        .start(batchJobRequest.getJobName(), batchJobRequest.getJobParameter());
                batchJobRequest.setJobExecutionId(jobExecutionId);
            } catch (NoSuchJobException | JobInstanceAlreadyExistsException | JobParametersInvalidException e) {
                logger.error("Job execution fail. [JobSeqId:{}][JobName:{}]", batchJobRequest.getJobSeqId(),
                        batchJobRequest.getJobName(), e);
            } finally {
                updateExecutionId(batchJobRequest);
            }
        }
    }

    /**
     * Update polling status of the batch job request table.
     * 
     * @param batchJobRequest Batch job request.
     * @return Success is true, failure is false.
     */
    boolean updateStatusPolled(BatchJobRequest batchJobRequest) {
        batchJobRequest.setPollingStatus(PollingStatus.POLLED);
        batchJobRequest.setUpdateDate(getTimestamp());
        String transactionName = "updatePollingStatusToPolled";

        // For update by optimistic locking, if the update is not performed, not issue a message as a quasi-normal.
        return updateJobRequestTable(batchJobRequest, PollingStatus.INIT, transactionName);
    }

    /**
     * Update job execution id and polling status of the batch job request table.
     *
     * @param batchJobRequest Batch job request.
     * @return Success is true, failure is false.
     */
    boolean updateExecutionId(BatchJobRequest batchJobRequest) {
        batchJobRequest.setPollingStatus(PollingStatus.EXECUTED);
        batchJobRequest.setUpdateDate(getTimestamp());
        String transactionName = "updateExecutionIdAndPollingStatusToExecuted";

        boolean result = updateJobRequestTable(batchJobRequest, PollingStatus.POLLED, transactionName);
        if (!result) {
            logger.warn("JobExecutionId update failed. [JobSeqId:{}][JobName:{}][JobExecutionId:{}]", batchJobRequest.getJobSeqId(),
                    batchJobRequest.getJobName(), batchJobRequest.getJobExecutionId());
        }
        return result;
    }

    /**
     * Update of batch job request table.
     * 
     * @param batchJobRequest Batch job request
     * @param pollingStatus Polling status of update condition.
     * @param transactionName Transaction name.
     * @return Success is true, failure is false.
     */
    boolean updateJobRequestTable(BatchJobRequest batchJobRequest, PollingStatus pollingStatus, String transactionName) {

        DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setName(transactionName);

        TransactionStatus status = transactionManager.getTransaction(definition);
        int result = 0;
        try {
            result = batchJobRequestRepository.updateStatus(batchJobRequest, pollingStatus);
            transactionManager.commit(status);
        } catch (Exception e) {
            logger.error("Update of batch job request table is fail.", e);
            transactionManager.rollback(status);
        }

        return result == 1;
    }

    /**
     * Setting the pollingQueryParams of the search condition for extracting the batch job request.
     * 
     * @param optionalPollingQueryParams The pollingQueryParams of the search condition.
     */
    public void setOptionalPollingQueryParams(Map<String, Object> optionalPollingQueryParams) {
        this.optionalPollingQueryParams = optionalPollingQueryParams;
    }

    /**
     * Setting the output message flag of executing poll method.
     * 
     * @param enablePollingLog true is output, false is not output.
     */
    public void setEnablePollingLog(boolean enablePollingLog) {
        this.enablePollingLog = enablePollingLog;
    }

    /**
     * Setting the clock.
     *
     * @param clock clock.
     */
    public void setClock(Clock clock) { this.clock = clock; }

    /**
     * Get a timestamp.
     * 
     * @return Timestamp.
     */
    protected Timestamp getTimestamp() { return new Timestamp(clock.millis()); }

    /**
     * To change the status during the shutdown preparation.
     */
    private void prepareShutdown() {
        shutdownCalled = true;
    }

    /**
     * Change the status so that it does not poll the batch job request.
     */
    @Override
    public void destroy() throws Exception {
        logger.info("JobRequestPollTask is called shutdown.");
        prepareShutdown();
    }

    /**
     * Initial processing.
     * <p>
     * Make the settings that you want to stop waiting for a certain period of time until the batch job is finished.
     * </p>
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        this.daemonTaskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        this.daemonTaskExecutor.setAwaitTerminationSeconds(awaitTerminationSeconds);

        pollingQueryParams.put("pollingRowLimit", pollingRowLimit);
        if (optionalPollingQueryParams != null) {
            pollingQueryParams.putAll(optionalPollingQueryParams);
        }

    }
}