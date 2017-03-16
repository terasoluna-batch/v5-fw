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
package org.terasoluna.batch.async.db.repository

import org.dbunit.Assertion
import org.dbunit.JdbcDatabaseTester
import org.dbunit.database.DatabaseConfig
import org.dbunit.dataset.ReplacementDataSet
import org.dbunit.dataset.filter.DefaultColumnFilter
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.transaction.BeforeTransaction
import org.terasoluna.batch.async.db.model.BatchJobRequest
import org.terasoluna.batch.async.db.model.PollingStatus
import org.terasoluna.batch.test.spock.dbunit.DataTableLoader
import spock.lang.Narrative
import spock.lang.Specification
import spock.lang.Unroll

import java.sql.Timestamp

/**
 * Test BatchJobRequestMapper
 *
 * 5.0.0
 */

@ContextConfiguration(locations = "classpath:META-INF/spring/polling-task.xml")
@Narrative("""
To access and mapping to the job request table.
It has provided the following operations
1.Specify a job request that has not been polling number of items acquired
2.Update the polling status of the job request table
3.Update the polling status and job execution ID of the job request table
""")
class BatchJobRequestMapperSpec extends Specification {

    @Autowired
    BatchJobRequestMapper batchJobRequestMapper

    @Autowired
    @Qualifier("appProperty")
    Properties appProperty

    String url
    String driver
    String username
    String password

    def tableName = "BATCH_JOB_REQUEST"


    @BeforeTransaction
    def setup() {
        driver = appProperty.getProperty("jdbc.driver")
        url = appProperty.getProperty("jdbc.url")
        username = appProperty.getProperty("jdbc.username")
        password = appProperty.getProperty("jdbc.password")
    }

    @Unroll
    def "Polling status to get the specified number of items(#limits) the job requirements of the INIT, result:#count"() {
        setup:
        def tester = new JdbcDatabaseTester(driver, url, username, password)
        def data = createDataSet {
            batch_job_request {
                job_seq_id | job_name | job_parameter | polling_status | job_execution_id | create_date  | update_date
                1          | "JOB01"  | "[null]"      | "EXECUTED"     | "[null]"         | "[now]"      | "1901-01-01"
                2          | "JOB02"  | "[null]"      | "INIT"         | "[null]"         | "[now]"      | "1901-01-01"
                3          | "JOB03"  | "[null]"      | "POLLED"       | "[null]"         | "[now]"      | "1901-01-01"
                4          | "JOB04"  | "[null]"      | "INIT"         | "[null]"         | "[now]"      | "1901-01-01"
                5          | "JOB05"  | "[null]"      | "POLLED"       | "[null]"         | "[now]"      | "1901-01-01"
                6          | "JOB06"  | "[null]"      | "INIT"         | "[null]"         | "[now]"      | "1901-01-01"
                7          | "JOB07"  | "[null]"      | "EXECUTED"     | "[null]"         | "[now]"      | "1901-01-01"
                8          | "JOB08"  | "[null]"      | "INIT"         | "[null]"         | "[now]"      | "1901-01-01"
                9          | "JOB09"  | "[null]"      | "EXECUTED"     | "[null]"         | "[now]"      | "1901-01-01"
            }
        }
        tester.dataSet = data
        tester.connection.config.setProperty(DatabaseConfig.FEATURE_ALLOW_EMPTY_FIELDS, true)
        tester.onSetup()
        tester.connection.connection.commit()

        expect:
        def params = ["pollingRowLimit":limits]

        def items = batchJobRequestMapper.find(params)
        items.size() == count


        where:
        limits | count
             1 | 1
             3 | 3
             4 | 4
             5 | 4
    }

    def "Polling status to get the job requirements of the INIT"() {
        setup:
        def tester = new JdbcDatabaseTester(driver, url, username, password)
        def data = createDataSet {
            batch_job_request {
                job_seq_id | job_name | job_parameter | polling_status | job_execution_id | create_date  | update_date
                1          | "JOB01"  | "[null]"      | "EXECUTED"     | "[null]"         | "[now]"      | "1901-01-01"
                2          | "JOB02"  | "[null]"      | "INIT"         | "[null]"         | "[now]"      | "1901-01-01"
                3          | "JOB03"  | "[null]"      | "POLLED"       | "[null]"         | "[now]"      | "1901-01-01"
                4          | "JOB04"  | "[null]"      | "INIT"         | "[null]"         | "[now]"      | "1901-01-01"
                5          | "JOB05"  | "[null]"      | "POLLED"       | "[null]"         | "[now]"      | "1901-01-01"
                6          | "JOB06"  | "[null]"      | "INIT"         | "[null]"         | "[now]"      | "1901-01-01"
                7          | "JOB07"  | "[null]"      | "INIT"         | "[null]"         | "[now]"      | "1901-01-01"
                8          | "JOB08"  | "[null]"      | "POLLED"       | "[null]"         | "[now]"      | "1901-01-01"
                9          | "JOB09"  | "[null]"      | "EXECUTED"     | "[null]"         | "[now]"      | "1901-01-01"
            }
        }
        tester.dataSet = data
        tester.connection.config.setProperty(DatabaseConfig.FEATURE_ALLOW_EMPTY_FIELDS, true)
        tester.onSetup()
        tester.connection.connection.commit()

        when:
        def params = ["pollingRowLimit":5]

        def items = batchJobRequestMapper.find(params)

        then:
        items.size() == 4
        items.pollingStatus.contains(PollingStatus.INIT)
        items.jobName.containsAll(["JOB02", "JOB04", "JOB06", "JOB07"])

    }

    @Unroll
    def "Update the polling status(#request.pollingStatus) of the job request table(job_seq_id:#request.jobSeqId), update(#count)"() {
        setup:
        def tester = new JdbcDatabaseTester(driver, url, username, password)
        def data = createDataSet {
            batch_job_request {
                job_seq_id | job_name | job_parameter | polling_status | job_execution_id | create_date  | update_date
                1          | "JOB01"  | "[null]"      | "EXECUTED"     | "[null]"         | "[now]"      | "1901-01-01"
                2          | "JOB02"  | "[null]"      | "INIT"         | "[null]"         | "[now]"      | "1901-01-01"
                3          | "JOB03"  | "[null]"      | "POLLED"       | "[null]"         | "[now]"      | "1901-01-01"
            }
        }
        tester.dataSet = data
        tester.connection.config.setProperty(DatabaseConfig.FEATURE_ALLOW_EMPTY_FIELDS, true)
        tester.onSetup()
        tester.connection.connection.commit()

        expect:
            count == batchJobRequestMapper.updateStatus(request, conditionStatus)

        where:
        request                                       | conditionStatus       || count
        crateJobRequest(2, PollingStatus.POLLED,  0)  | PollingStatus.INIT    || 1
        crateJobRequest(3, PollingStatus.EXECUTED, 0) | PollingStatus.POLLED  || 1
        crateJobRequest(1, PollingStatus.POLLED, 0)   | PollingStatus.INIT    || 0
    }

    def "Update the polling status of the job request table"() {
        setup:
        def tester = new JdbcDatabaseTester(driver, url, username, password)
        def data = createDataSet {
            batch_job_request {
                job_seq_id | job_name | job_parameter | polling_status | job_execution_id | create_date  | update_date
                1          | "JOB01"  | "[null]"      | "EXECUTED"     | "[null]"         | "1900-01-01" | "1901-01-01"
                2          | "JOB02"  | "[null]"      | "INIT"         | "[null]"         | "1900-01-01" | "1901-01-01"
                3          | "JOB03"  | "[null]"      | "POLLED"       | "[null]"         | "1900-01-01" | "1901-01-01"
            }
        }
        tester.dataSet = data
        tester.connection.config.setProperty(DatabaseConfig.FEATURE_ALLOW_EMPTY_FIELDS, true)
        tester.onSetup()
        tester.connection.connection.commit()
        def expectTable = createDataSet {
            batch_job_request {
                job_seq_id | job_name | job_parameter | polling_status | job_execution_id | create_date
                1          | "JOB01"  | "[null]"      | "EXECUTED"     | "[null]"         | "1900-01-01"
                2          | "JOB02"  | "[null]"      | "POLLED"       | "[null]"         | "1900-01-01"
                3          | "JOB03"  | "[null]"      | "POLLED"       | "[null]"         | "1900-01-01"
            }
        }.getTable("batch_job_request")

        when:
        def count = batchJobRequestMapper.updateStatus(crateJobRequest(2, PollingStatus.POLLED, null), PollingStatus.INIT)

        then:
        count == 1
        def actualTable = tester.connection.createTable(tableName)
        def filteredTable = DefaultColumnFilter.includedColumnsTable(actualTable, expectTable.tableMetaData.columns)
        Assertion.assertEquals(expectTable, filteredTable)
        "1901-01-01 00:00:00.0" == actualTable.getValue(0, "update_date").toString()
        "1901-01-01 00:00:00.0" != actualTable.getValue(1, "update_date").toString()
        "1901-01-01 00:00:00.0" == actualTable.getValue(2, "update_date").toString()

    }

    @Unroll
    def "Update the polling status(#request.pollingStatus) and job execution ID(#request.jobExecutionId) of the job request table(job_seq_id:#request.jobSeqId), update(#count)"() {
        setup:
        def tester = new JdbcDatabaseTester(driver, url, username, password)
        def data = createDataSet {
            batch_job_request {
                job_seq_id | job_name | job_parameter | polling_status | job_execution_id | create_date  | update_date
                1          | "JOB01"  | "[null]"      | "EXECUTED"     | "[null]"         | "[now]"      | "1901-01-01"
                2          | "JOB02"  | "[null]"      | "INIT"         | "[null]"         | "[now]"      | "1901-01-01"
                3          | "JOB03"  | "[null]"      | "POLLED"       | "[null]"         | "[now]"      | "1901-01-01"
            }
        }
        tester.dataSet = data
        tester.connection.config.setProperty(DatabaseConfig.FEATURE_ALLOW_EMPTY_FIELDS, true)
        tester.onSetup()
        tester.connection.connection.commit()

        expect:
        count == batchJobRequestMapper.updateStatus(request, conditionStatus)

        where:
        request                                         | conditionStatus       || count
        crateJobRequest(2, PollingStatus.POLLED,  100)  | PollingStatus.INIT    || 1
        crateJobRequest(3, PollingStatus.EXECUTED, 101) | PollingStatus.POLLED  || 1
        crateJobRequest(1, PollingStatus.POLLED, 102)   | PollingStatus.INIT    || 0
    }

    def "Update the polling status and job execution ID of the job request table"() {
        setup:
        def tester = new JdbcDatabaseTester(driver, url, username, password)
        def data = createDataSet {
            batch_job_request {
                job_seq_id | job_name | job_parameter | polling_status | job_execution_id | create_date  | update_date
                1          | "JOB01"  | "[null]"      | "EXECUTED"     | "[null]"         | "1900-01-01" | "1901-01-01"
                2          | "JOB02"  | "[null]"      | "INIT"         | "[null]"         | "1900-01-01" | "1901-01-01"
                3          | "JOB03"  | "[null]"      | "POLLED"       | "[null]"         | "1900-01-01"  | "1901-01-01"
            }
        }
        tester.dataSet = data
        tester.connection.config.setProperty(DatabaseConfig.FEATURE_ALLOW_EMPTY_FIELDS, true)
        tester.onSetup()
        tester.connection.connection.commit()
        def expectTable = createDataSet {
            batch_job_request {
                job_seq_id | job_name | job_parameter | polling_status | job_execution_id | create_date
                1          | "JOB01"  | "[null]"      | "EXECUTED"     | "[null]"         | "1900-01-01"
                2          | "JOB02"  | "[null]"      | "POLLED"       | 200              | "1900-01-01"
                3          | "JOB03"  | "[null]"      | "POLLED"       | "[null]"         | "1900-01-01"
            }
        }.getTable("batch_job_request")

        when:
        def count = batchJobRequestMapper.updateStatus(crateJobRequest(2, PollingStatus.POLLED, 200), PollingStatus.INIT)

        then:
        count == 1
        def actualTable = tester.connection.createTable(tableName)
        def filteredTable = DefaultColumnFilter.includedColumnsTable(actualTable, expectTable.tableMetaData.columns)
        Assertion.assertEquals(expectTable, filteredTable)
        "1901-01-01 00:00:00.0" == actualTable.getValue(0, "update_date").toString()
        "1901-01-01 00:00:00.0" != actualTable.getValue(1, "update_date").toString()
        "1901-01-01 00:00:00.0" == actualTable.getValue(2, "update_date").toString()

    }


    def crateJobRequest(long jobSeqId, PollingStatus status, Long jobExecutionId) {
        def request = new BatchJobRequest()
        request.jobSeqId = jobSeqId
        request.pollingStatus = status
        request.jobExecutionId = jobExecutionId
        request.updateDate = new Timestamp(System.currentTimeMillis())
        request
    }

    def createDataSet(Closure c) {
        def replacementDataSet = new ReplacementDataSet(DataTableLoader.loadDataSet(c))
        replacementDataSet.addReplacementObject("[null]", null)
        replacementDataSet.addReplacementObject("[now]", new Timestamp(System.currentTimeMillis()))
        replacementDataSet
    }
}

