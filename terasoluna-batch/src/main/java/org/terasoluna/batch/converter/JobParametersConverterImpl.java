/*
 * Copyright (C) 2023 NTT DATA Corporation
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
package org.terasoluna.batch.converter;

import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.converter.JobParametersConverter;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.AbstractJdbcBatchMetadataDao;
import org.springframework.batch.item.database.support.DataFieldMaxValueIncrementerFactory;
import org.springframework.batch.item.database.support.DefaultDataFieldMaxValueIncrementerFactory;
import org.springframework.batch.support.DatabaseType;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.support.incrementer.DataFieldMaxValueIncrementer;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Implementation of job parameter conversion.
 * 
 * An ID that identifies job execution is automatically assigned to job parameters.
 * 
 * @since 5.5.0
 */
public class JobParametersConverterImpl implements JobParametersConverter, InitializingBean {

    public static final String JOB_RUN_ID = "jsr_batch_run_id";
    public DataFieldMaxValueIncrementer incrementer;
    public String tablePrefix = AbstractJdbcBatchMetadataDao.DEFAULT_TABLE_PREFIX;
    public DataSource dataSource;
    DataFieldMaxValueIncrementerFactory factory;
    String incrementerType;

    /**
     * Create a new instance with the specified parameters.
     * 
     * @param dataSource used to access the database and get the unique id.
     */
    public JobParametersConverterImpl(DataSource dataSource) {
        Assert.notNull(dataSource, "A DataSource is required");
        this.dataSource = dataSource;
    }

    /**
     * The table prefix used in the current {@link JobRepository}
     *
     * @param tablePrefix used for the job repository tables.
     */
    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (this.factory == null) {
            this.factory = new DefaultDataFieldMaxValueIncrementerFactory(dataSource);
        }
        if (this.incrementerType == null) {
            this.incrementerType = DatabaseType.fromMetaData(dataSource).name();
        }
        this.incrementer = this.factory.getIncrementer(this.incrementerType, tablePrefix + "JOB_SEQ");
    }

    /**
     * Convert arguments to JobParameters.
     *
     * @param properties Job parameters(Properties object).
     * @return Job parameters.
     */
    @Override
    public JobParameters getJobParameters(@Nullable Properties properties) {
        JobParametersBuilder builder = new JobParametersBuilder();
        boolean runIdFound = false;

        if(properties != null) {
            for (Map.Entry<Object, Object> curParameter : properties.entrySet()) {
                if(curParameter.getValue() != null) {
                    if(curParameter.getKey().equals(JOB_RUN_ID)) {
                        runIdFound = true;
                        builder.addLong(curParameter.getKey().toString(), Long.valueOf((String) curParameter.getValue()), true);
                    } else {
                        builder.addString(curParameter.getKey().toString(), curParameter.getValue().toString(), false);
                    }
                }
            }
        }

        if(!runIdFound) {
            builder.addLong(JOB_RUN_ID, incrementer.nextLongValue());
        }

        return builder.toJobParameters();
    }

    /**
     * Convert arguments to Properties.
     *
     * @param params Job parameters(JobParameters object).
     * @return Job parameters.
     */
    @Override
    public Properties getProperties(@Nullable JobParameters params) {
        Properties properties = new Properties();
        boolean runIdFound = false;

        if(params != null) {
            for(Map.Entry<String, JobParameter<?>> curParameter: params.getParameters().entrySet()) {
                if(curParameter.getKey().equals(JOB_RUN_ID)) {
                    runIdFound = true;
                }

                properties.setProperty(curParameter.getKey(), curParameter.getValue().getValue().toString());
            }
        }

        if(!runIdFound) {
            properties.setProperty(JOB_RUN_ID, String.valueOf(incrementer.nextLongValue()));
        }

        return properties;
    }
}
