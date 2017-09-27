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
package org.terasoluna.batch.async.db

import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobParametersInvalidException
import org.springframework.batch.core.UnexpectedJobExecutionException
import org.springframework.batch.core.configuration.support.AutomaticJobRegistrar
import org.springframework.batch.core.launch.JobExecutionNotRunningException
import org.springframework.batch.core.launch.JobInstanceAlreadyExistsException
import org.springframework.batch.core.launch.JobOperator
import org.springframework.batch.core.launch.JobParametersNotFoundException
import org.springframework.batch.core.launch.NoSuchJobException
import org.springframework.batch.core.launch.NoSuchJobExecutionException
import org.springframework.batch.core.launch.NoSuchJobInstanceException
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException
import org.springframework.batch.core.repository.JobRestartException
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.config.AutowireCapableBeanFactory
import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext
import org.springframework.core.task.TaskRejectedException
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.test.context.ContextConfiguration
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionStatus
import org.springframework.util.ClassUtils
import org.terasoluna.batch.async.db.model.BatchJobRequest
import org.terasoluna.batch.async.db.model.PollingStatus
import org.terasoluna.batch.async.db.repository.BatchJobRequestRepository
import spock.lang.Narrative
import spock.lang.Specification
import spock.lang.Unroll
import uk.org.lidalia.slf4jext.Level
import uk.org.lidalia.slf4jtest.LoggingEvent
import uk.org.lidalia.slf4jtest.TestLoggerFactory

import java.sql.Timestamp

import static org.hamcrest.CoreMatchers.hasItem
import static spock.util.matcher.HamcrestSupport.that
/**
 * Test JobRequestPollTask
 *
 * @since 5.0.0
 */
@ContextConfiguration(locations = "classpath:META-INF/spring/polling-task.xml")
@Narrative("""
To get the job request table only simultaneous run for a few minutes in the specified period.
In order to perform the jobs that have been specified in the job request,
a thread of execution to the asynchronous execution.
To update the polling status of the job request table in the "POLLED" at that time.
Run synchronize the job is a thread of execution, to update the job execution ID and
the polling status in the "EXECUTED" after the job completion.
Ignore the request that exceeds the number of concurrent execution, and the processing in the next poll.
When the asynchronous batch daemon is stopped, scheduled polling process is not performed
""")
class JobRequestPollTaskSpec extends Specification {

    @Autowired
    ThreadPoolTaskExecutor daemonTaskExecutor

    @Autowired
    ApplicationContext context

    @Autowired
    @Qualifier("appProperty")
    Properties appProperty

    def batchJobRequestRepository = Mock(BatchJobRequestRepository)

    def transactionManager = Mock(PlatformTransactionManager)

    def jobOperator = Mock(JobOperator)

    def automaticJobRegistrar = Mock(AutomaticJobRegistrar) {
        isRunning() >> true
    }

    def logger = TestLoggerFactory.getTestLogger(JobRequestPollTask.class)

    def cleanup() {
        TestLoggerFactory.clearAll()
    }


    def "One of the essential argument 'BatchJobRequestRepository' is the null, to create an instance"() {
        when:
        new JobRequestPollTask(null, transactionManager, daemonTaskExecutor, jobOperator, automaticJobRegistrar)
        then:
        def ex = thrown(IllegalArgumentException)
        ex.getMessage() == "batchJobRequestRepository must be not null."
    }

    def "One of the essential argument 'TransactionManager' is the null, to create an instance"() {
        when:
        new JobRequestPollTask(batchJobRequestRepository, null, daemonTaskExecutor, jobOperator, automaticJobRegistrar)
        then:
        def ex = thrown(IllegalArgumentException)
        ex.getMessage() == "transactionManager must be not null."
    }

    def "One of the essential argument 'daemonTaskExecutor' is the null, to create an instance"() {
        when:
        new JobRequestPollTask(batchJobRequestRepository, transactionManager, null, jobOperator, automaticJobRegistrar)
        then:
        def ex = thrown(IllegalArgumentException)
        ex.getMessage() == "daemonTaskExecutor must be not null."
    }

    def "One of the essential argument 'JobOperator' is the null, to create an instance"() {
        when:
        new JobRequestPollTask(batchJobRequestRepository, transactionManager, daemonTaskExecutor, null, automaticJobRegistrar)
        then:
        def ex = thrown(IllegalArgumentException)
        ex.getMessage() == "jobOperator must be not null."
    }

    def "One of the essential argument 'AutomaticJobRegistrar' is the null, to create an instance"() {
        when:
        new JobRequestPollTask(batchJobRequestRepository, transactionManager, daemonTaskExecutor, jobOperator, null)
        then:
        def ex = thrown(IllegalArgumentException)
        ex.getMessage() == "automaticJobRegistrar must be not null."
    }

    def "Setting polling task after Bean generation is completed"() {
        setup:
        def taskExecutorMock = Spy(ThreadPoolTaskExecutor)
        def task = new JobRequestPollTask(batchJobRequestRepository, transactionManager, taskExecutorMock, jobOperator, automaticJobRegistrar)
        context.getAutowireCapableBeanFactory().autowireBeanProperties(task, AutowireCapableBeanFactory.AUTOWIRE_BY_TYPE, false)
        task.@optionalPollingQueryParams =  ["param1":"1", "Param2":2]

        when:
        task.afterPropertiesSet()
        then:
        1 * taskExecutorMock.setAwaitTerminationSeconds(appProperty.getProperty("async-batch-daemon.job-await-termination-seconds").toInteger())
        1 * taskExecutorMock.setWaitForTasksToCompleteOnShutdown(true)
        task.@pollingQueryParams == ["pollingRowLimit":task.@pollingRowLimit, "param1":"1", "Param2":2]
    }

    def "To stop the polling process when the Bean is destroyed by the daemon stop"() {
        setup:
        def task = new JobRequestPollTask(batchJobRequestRepository, transactionManager, daemonTaskExecutor, jobOperator, automaticJobRegistrar)
        when:
        task.destroy()
        then:
        task.@shutdownCalled == true
    }

    def "To set the query parameters"() {
        setup:
        def task = new JobRequestPollTask(batchJobRequestRepository, transactionManager, daemonTaskExecutor, jobOperator, automaticJobRegistrar)
        def paramDate = new Date()
        def paramMap = ["param1":"1", "Param2":2, "Params3":paramDate]
        when:
        task.setOptionalPollingQueryParams(paramMap)
        then:
        def pollingQueryParams = task.@optionalPollingQueryParams
        pollingQueryParams != null
        pollingQueryParams == paramMap
    }

    def "To perform the job to get the job request"() {
        setup:
        def count = 3
        List<BatchJobRequest> jobRequests = createRequest(count)
        def transactionStatusMock = Mock(TransactionStatus)
        def task = new JobRequestPollTask(batchJobRequestRepository, transactionManager, daemonTaskExecutor, jobOperator, automaticJobRegistrar)
        task.@pollingRowLimit = count

        when:
        task.poll()
        sleep(1000L)

        then:
        1 * batchJobRequestRepository.find(_) >> jobRequests
        (count * 2) * batchJobRequestRepository.updateStatus(_,_) >> 1
        count * jobOperator.start(_,_) >>> [1,2,3]
        (count * 2 + 1) * transactionManager.getTransaction(_) >> transactionStatusMock
        (count * 2 + 1) * transactionManager.commit(_)
        0 * transactionManager.rollback(_)
    }

    def "Not do anything if there is no job request"() {
        setup:
        def count = 0
        List<BatchJobRequest> jobRequests = []
        def transactionStatusMock = Mock(TransactionStatus)
        def task = new JobRequestPollTask(batchJobRequestRepository, transactionManager, daemonTaskExecutor, jobOperator, automaticJobRegistrar)
        task.@pollingRowLimit = count

        when:
        task.poll()
        sleep(1000L)

        then:
        1 * batchJobRequestRepository.find(_) >> jobRequests
        (count * 2) * batchJobRequestRepository.updateStatus(_,_) >> 1
        count * jobOperator.start(_,_) >>> [1,2,3]
        (count * 2 + 1) * transactionManager.getTransaction(_) >> transactionStatusMock
        (count * 2 + 1) * transactionManager.commit(_)
        0 * transactionManager.rollback(_)
    }

    def "If you can not schedule the job execution, not also update the status without executing the job"() {
        setup:
        def count = 3
        List<BatchJobRequest> jobRequests = createRequest(count)
        def transactionStatusMock = Mock(TransactionStatus)
        def executorSpy = Spy(ThreadPoolTaskExecutor)
        executorSpy.corePoolSize = count
        executorSpy.maxPoolSize = count
        executorSpy.queueCapacity = -1
        executorSpy.initialize()
        executorSpy.execute(_) >> {callRealMethod()} >> {throw new TaskRejectedException("reject")} >> {callRealMethod()}

        def task = new JobRequestPollTask(batchJobRequestRepository, transactionManager, executorSpy, jobOperator, automaticJobRegistrar)
        task.@pollingRowLimit = count


        def logDebug = LoggingEvent.debug("Concurrency number of executing job is over, and skip this and after requests. [{}]", jobRequests.get(1))
        logger.setEnabledLevels(Level.ERROR,Level.DEBUG)

        when:
        task.poll()
        sleep(1000L)

        then:
        1 * batchJobRequestRepository.find(_) >> jobRequests
        2 * batchJobRequestRepository.updateStatus(_,_) >> 1
        1 * jobOperator.start(_,_) >>> 1L
        3 * transactionManager.getTransaction(_) >> transactionStatusMock
        3 * transactionManager.commit(_)
        0 * transactionManager.rollback(_)
        jobRequests.get(0).pollingStatus == PollingStatus.EXECUTED
        jobRequests.get(1).pollingStatus == PollingStatus.INIT
        jobRequests.get(2).pollingStatus == PollingStatus.INIT
        that logger.getLoggingEvents(), hasItem(logDebug)
    }

    def "If it has been locked by the update by optimistic locking will not execute the job, the status is also not updated"() {
        setup:
        def count = 1
        List<BatchJobRequest> jobRequests = createRequest(count)
        def transactionStatusMock = Mock(TransactionStatus)
        def task = new JobRequestPollTask(batchJobRequestRepository, transactionManager, daemonTaskExecutor, jobOperator, automaticJobRegistrar)
        task.@pollingRowLimit = count

        when:
        task.poll()
        sleep(1000L)

        then:
        1 * batchJobRequestRepository.find(_) >> jobRequests
        1 * batchJobRequestRepository.updateStatus(_,_) >>> [0, 1]
        0 * jobOperator.start(_,_) >>> [1,2,3]
        2 * transactionManager.getTransaction(_) >> transactionStatusMock
        2 * transactionManager.commit(_)
        0 * transactionManager.rollback(_)
    }

    def "If an exception is thrown in the job execution, it performs the normal processing, to update the polling status"() {
        setup:
        def count = 7
        List<BatchJobRequest> jobRequests = createRequest(count)
        def transactionStatusMock = Mock(TransactionStatus)
        jobOperator.start(_, _) >>> 1 >> {
            throw new NoSuchJobException("job not found.")
        } >> 2 >> {
            throw new JobInstanceAlreadyExistsException("Already exists.")
        } >> 3 >> {
            throw new JobParametersInvalidException("invalid pollingQueryParams")
        } >> 4
        def taskExecutor = new ThreadPoolTaskExecutor()
        taskExecutor.corePoolSize = count
        taskExecutor.maxPoolSize = count
        taskExecutor.queueCapacity = -1
        taskExecutor.initialize()
        def task = new JobRequestPollTask(batchJobRequestRepository, transactionManager, taskExecutor, jobOperator, automaticJobRegistrar)
        task.@pollingRowLimit = count

        when:
        task.poll()
        sleep(1000L)

        then:
        1 * batchJobRequestRepository.find(_) >> jobRequests
        (count * 2) * batchJobRequestRepository.updateStatus(_,_) >> 1
        (count * 2 + 1) * transactionManager.getTransaction(_) >> transactionStatusMock
        (count * 2 + 1) * transactionManager.commit(_)
        0 * transactionManager.rollback(_)
        jobRequests.count({it.jobExecutionId == null}) == 3
        logger.getAllLoggingEvents().size() == 4
        // Since an exception occurs in any job different on every execution, inspection of the argument, the exception of the log is omitted.
        logger.allLoggingEvents.subList(1,3).each {
            assert it.level == Level.ERROR
            assert it.message == "Job execution fail. [JobSeqId:{}][JobName:{}]"
        }

    }

    def "If a failure in the status update has occurred, do not run the job of unexecuted to roll back"() {
        setup:
        def count = 1
        List<BatchJobRequest> jobRequests = createRequest(count)
        def transactionStatusMock = Mock(TransactionStatus)
        def task = new JobRequestPollTask(batchJobRequestRepository, transactionManager, daemonTaskExecutor, jobOperator, automaticJobRegistrar)
        task.@pollingRowLimit = count

        when:
        task.poll()
        sleep(1000L)

        then:
        1 * batchJobRequestRepository.find(_) >> jobRequests
        1 * batchJobRequestRepository.updateStatus(_,_) >> {throw new RuntimeException("db access error.")} >> 1
        0 * jobOperator.start(_,_) >>> 1
        2 * transactionManager.getTransaction(_) >> transactionStatusMock
        1 * transactionManager.commit(_)
        1 * transactionManager.rollback(_)
        logger.allLoggingEvents.size() == 2
        logger.allLoggingEvents.get(1).message == "Update of batch job request table is fail."

    }

    def "When you fail to update the JobExecutionId outputs a warning log"() {
        setup:
        def count = 1
        List<BatchJobRequest> jobRequests = createRequest(count)
        def transactionStatusMock = Mock(TransactionStatus)
        def task = new JobRequestPollTask(batchJobRequestRepository, transactionManager, daemonTaskExecutor, jobOperator, automaticJobRegistrar)
        task.@pollingRowLimit = count

        def warnLog = LoggingEvent.warn("JobExecutionId update failed. [JobSeqId:{}][JobName:{}]",
                jobRequests.get(0).jobSeqId, jobRequests.get(0).jobName)

        when:
        task.poll()
        sleep(1000L)

        then:
        1 * batchJobRequestRepository.find(_) >> jobRequests
        2 * batchJobRequestRepository.updateStatus(_,_) >>> [1, 0]
        1 * jobOperator.start(_,_) >>> 1
        3 * transactionManager.getTransaction(_) >> transactionStatusMock
        3 * transactionManager.commit(_)
        0 * transactionManager.rollback(_)
        logger.allLoggingEvents.size() == 2
        that logger.allLoggingEvents, hasItem(warnLog)
    }


    def "In the case of daemons stop state does not perform the polling process"() {
        setup:
        def count = 1
        List<BatchJobRequest> jobRequests = createRequest(count)
        def transactionStatusMock = Mock(TransactionStatus)
        def task = new JobRequestPollTask(batchJobRequestRepository, transactionManager, daemonTaskExecutor, jobOperator, automaticJobRegistrar)
        task.@pollingRowLimit = count
        task.@shutdownCalled = true

        when:
        task.poll()
        sleep(1000L)

        then:
        0 * batchJobRequestRepository.find(_) >> jobRequests
        0 * batchJobRequestRepository.updateStatus(_,_) >> {throw new RuntimeException("db access error.")}
        0 * jobOperator.start(_,_) >>> 1
        0 * transactionManager.getTransaction(_) >> transactionStatusMock
        0 * transactionManager.commit(_)
        0 * transactionManager.rollback(_)
    }

    def "Setting property values is set in the variable"() {
        setup:
        def configLocation = ClassUtils.addResourcePathToPackagePath(JobRequestPollTaskSpec.class, "not-default-value-polling-task.xml")

        when:
        ApplicationContext context = new ClassPathXmlApplicationContext(configLocation)
        JobRequestPollTask task = context.getBean(JobRequestPollTask.class)

        then:
        task.@pollingRowLimit == 123
        task.@awaitTerminationSeconds == 987

        cleanup:
        context.close()
    }

    def "If no setting property values, a default value is set in the variable"() {
        setup:
        def configLocation = ClassUtils.addResourcePathToPackagePath(JobRequestPollTaskSpec.class, "default-value-polling-task.xml")

        when:
        ApplicationContext context = new ClassPathXmlApplicationContext(configLocation)
        JobRequestPollTask task = context.getBean(JobRequestPollTask.class)

        then:
        task.@pollingRowLimit == 3
        task.@awaitTerminationSeconds == 600

        cleanup:
        context.close()
    }

    def "If automaticJobRegistrar is not on running status, must be discontinued polling process."() {
        setup:
        def count = 1
        List<BatchJobRequest> jobRequests = createRequest(count)
        def transactionStatusMock = Mock(TransactionStatus)
        def localAutomaticJobRegistrar = Mock(AutomaticJobRegistrar) {
            1 * isRunning() >> false
        }
        def task = new JobRequestPollTask(batchJobRequestRepository, transactionManager, daemonTaskExecutor, jobOperator, localAutomaticJobRegistrar)
        task.@pollingRowLimit = count

        when:
        task.poll()
        sleep(1000L)

        then:
        0 * batchJobRequestRepository.find(_) >> jobRequests
        0 * batchJobRequestRepository.updateStatus(_,_) >>> [0, 1]
        0 * jobOperator.start(_,_) >>> [1,2,3]
        0 * transactionManager.getTransaction(_) >> transactionStatusMock
        0 * transactionManager.commit(_)
        0 * transactionManager.rollback(_)
        logger.allLoggingEvents.size() == 2
        that logger.allLoggingEvents, hasItem(LoggingEvent.info("Put off polling, because jobRegistry is not on running status."))
    }

    @Unroll
    def "Check polling log output switch (enablePollingLog: #enablePollingLog, Output: #output)"() {
        setup:
        def count = 0
        List<BatchJobRequest> jobRequests = []
        def task = new JobRequestPollTask(batchJobRequestRepository, transactionManager, daemonTaskExecutor, jobOperator, automaticJobRegistrar)
        task.@pollingRowLimit = count

        batchJobRequestRepository.find(_) >> jobRequests

        expect:
        task.setEnablePollingLog(enablePollingLog)
        task.poll()
        (logger.getAllLoggingEvents().message.find { it == "Polling processing." } != null) == output

        where:
        enablePollingLog || output
        true             || true
        false            || false
    }

    def createRequest(int num) {
        def requests = []
        1.upto(num, {
            def request = new BatchJobRequest()
            request.jobSeqId = it
            request.jobName = String.format("JOB%03d", (it % 1000))
            request.jobParameter = "param1=${it}"
            request.pollingStatus = PollingStatus.INIT
            request.jobExecutionId = null
            request.createDate = new Timestamp(System.currentTimeMillis())
            request.updateDate = null
            requests << request
        })
        requests
    }
}

class NoopJobOperator implements JobOperator {
    @Override
    List<Long> getExecutions(long l) throws NoSuchJobInstanceException {
        return null
    }

    @Override
    List<Long> getJobInstances(String s, int i, int i1) throws NoSuchJobException {
        return null
    }

    @Override
    Set<Long> getRunningExecutions(String s) throws NoSuchJobException {
        return null
    }

    @Override
    String getParameters(long l) throws NoSuchJobExecutionException {
        return null
    }

    @Override
    Long start(String s, String s1) throws NoSuchJobException, JobInstanceAlreadyExistsException, JobParametersInvalidException {
        return null
    }

    @Override
    Long restart(long l) throws JobInstanceAlreadyCompleteException, NoSuchJobExecutionException, NoSuchJobException, JobRestartException, JobParametersInvalidException {
        return null
    }

    @Override
    Long startNextInstance(String s) throws NoSuchJobException, JobParametersNotFoundException, JobRestartException, JobExecutionAlreadyRunningException, JobInstanceAlreadyCompleteException, UnexpectedJobExecutionException, JobParametersInvalidException {
        return null
    }

    @Override
    boolean stop(long l) throws NoSuchJobExecutionException, JobExecutionNotRunningException {
        return false
    }

    @Override
    String getSummary(long l) throws NoSuchJobExecutionException {
        return null
    }

    @Override
    Map<Long, String> getStepExecutionSummaries(long l) throws NoSuchJobExecutionException {
        return null
    }

    @Override
    Set<String> getJobNames() {
        return null
    }

    @Override
    JobExecution abandon(long l) throws NoSuchJobExecutionException, JobExecutionAlreadyRunningException {
        return null
    }
}
