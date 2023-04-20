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
package org.terasoluna.batch.converter

import org.apache.commons.dbcp2.BasicDataSource
import org.springframework.batch.core.JobParameter
import org.springframework.batch.core.JobParameters
import org.springframework.batch.item.database.support.DefaultDataFieldMaxValueIncrementerFactory
import org.springframework.jdbc.CannotGetJdbcConnectionException
import org.springframework.jdbc.support.incrementer.PostgresSequenceMaxValueIncrementer
import spock.lang.Specification

/**
 * Test JobParametersConverterImpl
 *
 * @since 5.5.0
 */
class JobParametersConverterImplSpec extends Specification {

    def ds = Mock(BasicDataSource)

    def "Throw exception when JobParametersConverterImpl constructor parameter 'dataSource' is null."() {
        when:
        def converter = new JobParametersConverterImpl(null)

        then:
        def ex = thrown(IllegalArgumentException)
        ex.getMessage() == "A DataSource is required"
    }

    def "incrementer is set when 'factory' is null before executing afterPropertiesSet-method."() {
        setup:
        def converter = new JobParametersConverterImpl(ds)
        converter.setTablePrefix("BATCH_")

        when:
        converter.incrementerType = "POSTGRES"
        converter.afterPropertiesSet()

        then:
        converter.incrementer.toString().contains("PostgresSequenceMaxValueIncrementer")

        when:
        converter.incrementerType = "ORACLE"
        converter.afterPropertiesSet()

        then:
        converter.incrementer.toString().contains("OracleSequenceMaxValueIncrementer")
    }

    def "incrementer is set when 'factory' is not null before executing afterPropertiesSet-method."() {
        setup:
        def converter = new JobParametersConverterImpl(ds)
        converter.setTablePrefix("BATCH_")
        converter.factory = new DefaultDataFieldMaxValueIncrementerFactory(ds)

        when:
        converter.incrementerType = "POSTGRES"
        converter.afterPropertiesSet()

        then:
        converter.incrementer.toString().contains("PostgresSequenceMaxValueIncrementer")

        when:
        converter.incrementerType = "ORACLE"
        converter.afterPropertiesSet()

        then:
        converter.incrementer.toString().contains("OracleSequenceMaxValueIncrementer")
    }

    def "jsr_batch_run_id is set when getJobParameters-method parameter 'properties' is not null."() {
        setup:
        def props = new Properties()
        def props_org = new Properties()
        props_org.setProperty("param1", "value1")
        props_org.setProperty("param2", "value2")
        def params
        def converter = new JobParametersConverterImpl(ds)
        def incrementerMock = Mock(PostgresSequenceMaxValueIncrementer)
        converter.incrementer = incrementerMock
        converter.setTablePrefix("BATCH_")
        incrementerMock.nextLongValue() >> 1

        when:
        params = converter.getJobParameters(props_org)

        then:
        for (Map.Entry<String, JobParameter> curParameter: params.getParameters().entrySet()) {
            props.setProperty(curParameter.getKey(), curParameter.getValue().getValue().toString())
        }
        props.getProperty("param1") == props_org.getProperty("param1")
        props.getProperty("param2") == props_org.getProperty("param2")
        props.size() == props_org.size() + 1
        1 * incrementerMock.nextLongValue() >> 1
    }

    def "jsr_batch_run_id is not set when getJobParameters-method parameter 'properties' is not null."() {
        setup:
        def props = new Properties()
        def props_org = new Properties()
        props_org.setProperty("jsr_batch_run_id", "1")
        props_org.setProperty("param1", "value1")
        props_org.setProperty("param2", "value2")
        def params
        def converter = new JobParametersConverterImpl(ds)
        def incrementerMock = Mock(PostgresSequenceMaxValueIncrementer)
        converter.incrementer = incrementerMock
        converter.setTablePrefix("BATCH_")
        incrementerMock.nextLongValue() >> 1

        when:
        params = converter.getJobParameters(props_org)

        then:
        for (Map.Entry<String, JobParameter> curParameter: params.getParameters().entrySet()) {
            props.setProperty(curParameter.getKey(), curParameter.getValue().getValue().toString())
        }
        props.getProperty("jsr_batch_run_id") == props_org.getProperty("jsr_batch_run_id")
        props.getProperty("param1") == props_org.getProperty("param1")
        props.getProperty("param2") == props_org.getProperty("param2")
        props.size() == props_org.size()
        0 * incrementerMock.nextLongValue() >> 1
    }

    def "jsr_batch_run_id is set when getJobParameters-method parameter 'properties' is null."() {
        setup:
        def params
        def converter = new JobParametersConverterImpl(ds)
        def incrementerMock = Mock(PostgresSequenceMaxValueIncrementer)
        converter.incrementer = incrementerMock
        converter.setTablePrefix("BATCH_")
        incrementerMock.nextLongValue() >> 1

        when:
        params = converter.getJobParameters(null)

        then:
        params.getParameters().size() == 1
        1 * incrementerMock.nextLongValue() >> 1
    }

    def "jsr_batch_run_id is set when getProperties-method parameter 'params' is not null."() {
        setup:
        def props
        def props_org = new LinkedHashMap()
        props_org.put("param1", new JobParameter("value1", String.class))
        props_org.put("param2", new JobParameter("value2", String.class))
        def params = new JobParameters(props_org)
        def converter = new JobParametersConverterImpl(ds)
        def incrementerMock = Mock(PostgresSequenceMaxValueIncrementer)
        converter.incrementer = incrementerMock
        converter.setTablePrefix("BATCH_")
        incrementerMock.nextLongValue() >> 1

        when:
        props = converter.getProperties(params)

        then:
        props.getProperty("param1") == (String)(props_org.get("param1").getValue())
        props.getProperty("param2") == (String)(props_org.get("param2").getValue())
        props.size() == props_org.size() + 1
        1 * incrementerMock.nextLongValue() >> 1
    }

    def "jsr_batch_run_id is not set when getProperties-method parameter 'params' is not null."() {
        setup:
        def props
        def props_org = new LinkedHashMap()
        props_org.put("jsr_batch_run_id", new JobParameter("1", String.class))
        props_org.put("param1", new JobParameter("value1", String.class))
        props_org.put("param2", new JobParameter("value2", String.class))
        def params = new JobParameters(props_org)
        def converter = new JobParametersConverterImpl(ds)
        def incrementerMock = Mock(PostgresSequenceMaxValueIncrementer)
        converter.incrementer = incrementerMock
        converter.setTablePrefix("BATCH_")
        incrementerMock.nextLongValue() >> 1

        when:
        props = converter.getProperties(params)

        then:
        props.getProperty("jsr_batch_run_id") == (String)(props_org.get("jsr_batch_run_id").getValue())
        props.getProperty("param1") == (String)(props_org.get("param1").getValue())
        props.getProperty("param2") == (String)(props_org.get("param2").getValue())
        props.size() == props_org.size()
        0 * incrementerMock.nextLongValue() >> 1
    }

    def "jsr_batch_run_id is set when getProperties-method parameter 'params' is null."() {
        setup:
        def props
        def converter = new JobParametersConverterImpl(ds)
        def incrementerMock = Mock(PostgresSequenceMaxValueIncrementer)
        converter.incrementer = incrementerMock
        converter.setTablePrefix("BATCH_")
        incrementerMock.nextLongValue() >> 1

        when:
        props = converter.getProperties(null)

        then:
        props.size() == 1
        1 * incrementerMock.nextLongValue() >> 1
    }
}