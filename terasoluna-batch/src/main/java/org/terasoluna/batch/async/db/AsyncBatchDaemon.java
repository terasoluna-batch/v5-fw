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
import org.springframework.batch.core.launch.support.JvmSystemExiter;
import org.springframework.batch.core.launch.support.SystemExiter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import jakarta.inject.Inject;
import java.io.File;
import java.nio.file.*;
import java.util.Objects;
import static java.net.URLClassLoader.*;

/**
 * Async Batch Daemon is basic daemon to launch a job request polling task.
 * <p>
 * This daemon is launched a job request polling task and monitoring specified a file to stop task and oneself. Daemon requires
 * a application context containing of a job request polling task.
 * The default application context is
 * "org.terasoluna.batch.config.AsyncBatchDaemonConfig"(Java Config)  or "/META-INF/spring/async-batch-daemon.xml"(XML Config)
 * on the class path.
 * And application context will automatically activate the profile of "async".
 * </p>
 * <p>
 * The monitoring file must be set in the key of "async-batch-daemon.polling-stop-file-path"in a application property file. This
 * property value is to be set by the annotation, it is necessary to define the {@code <context:annotation-config />} in the
 * configuration file of the application context.
 * </p>
 * <p>
 * The arguments to this class can be provided on the command line.
 * </p>
 * <p>
 * The command line options are as follows
 * </p>
 * <ul>
 * <li>configLocation: the xml application context containing a customized polling task.</li>
 * </ul>
 *
 * @since 5.0.0
 */
public class AsyncBatchDaemon {

    /**
     * logger.
     */
    private static final Logger logger = LoggerFactory.getLogger(AsyncBatchDaemon.class);

    /**
     * The default path of the configuration file to start the daemon(Java Config).
     */
    private static final String DEFAULT_CONFIG_LOCATION_JAVACONFIG = "org.terasoluna.batch.config.AsyncBatchDaemonConfig";
    
    /**
     * The default path of the configuration file to start the daemon(XML Config).
     */
    private static final String DEFAULT_CONFIG_LOCATION_XMLCONFIG = "/META-INF/spring/async-batch-daemon.xml";

    /**
     * Exit code at the time of the failure of a daemon.
     */
    private static final int FAILURE_STATUS = 255;

    /**
     * System Exiter.
     */
    private static SystemExiter systemExiter = new JvmSystemExiter();

    /**
     * Task executor for handling threads to perform the job.
     */
    @Inject
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    /**
     * Task scheduler for processing jobs at regular intervals.
     */
    @Inject
    private ThreadPoolTaskScheduler threadPoolTaskScheduler;

    /**
     * Monitoring specified a file path.
     */
    @Value("${async-batch-daemon.polling-stop-file-path:#{null}}")
    private String pollingStopFilePath;

    /**
     * Load Application Context.
     *
     * @param configLocation The application context containing a polling task (e.g. {@link JobRequestPollTask}).
     * @return Application Context.
     */
    ConfigurableApplicationContext loadContext(String configLocation) throws ClassNotFoundException {
        configLocation = determineContextPath(configLocation, ClassUtils.getDefaultClassLoader());
        logger.debug("configLocation:{}", configLocation);
        if (configLocation.endsWith(".xml")) {
            // for XML Config
            logger.debug("recognized XML Config.");
            final ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[] { configLocation }, false);
            context.getEnvironment().setActiveProfiles("async");
            context.refresh();
            return context;
        }
        // for Java Config
        logger.debug("recognized Java Config.");
        AnnotationConfigApplicationContext annotationConfigApplicationContext =
                new AnnotationConfigApplicationContext();
        Class<?> componentClass = annotationConfigApplicationContext.getClassLoader().loadClass(configLocation);
        annotationConfigApplicationContext.getEnvironment().setActiveProfiles("async");
        annotationConfigApplicationContext.register(componentClass);
        annotationConfigApplicationContext.refresh();
        return annotationConfigApplicationContext;
    }

    /**
     * Determine application context path.
     *
     * @param configLocation Application context path specified in the argument.
     * @param loader Class loader.
     * @return Application context path.
     */
    String determineContextPath(String configLocation, ClassLoader loader) {
        if (Objects.nonNull(configLocation)) {
            return configLocation;
        }
        if (ClassUtils.isPresent(DEFAULT_CONFIG_LOCATION_JAVACONFIG, loader)) {
            return DEFAULT_CONFIG_LOCATION_JAVACONFIG;
        }
        return DEFAULT_CONFIG_LOCATION_XMLCONFIG;
    }

    /**
     * Start a async batch daemon.
     *
     * @param configLocation The application context containing a polling task (e.g. {@link JobRequestPollTask}).
     * @return Exit status. Return zero on normal exit but return 255 on abnormal exit.
     * @see AsyncBatchDaemon#main(String[])
     */
    int start(String configLocation) {

        logger.info("Async Batch Daemon start.");

        WatchKey watchKey = null;
        WatchKey detectedWatchKey = null;

        try (ConfigurableApplicationContext context = loadContext(configLocation)) {
            context.registerShutdownHook();
            context.getAutowireCapableBeanFactory().autowireBeanProperties(this,
                    AutowireCapableBeanFactory.AUTOWIRE_BY_TYPE, false);

            int checkResult = checkSpecifiedFilePath();
            if (checkResult != 0) {
                return checkResult;
            }
            Path filePath = new File(pollingStopFilePath).toPath();
            Path dirPath = filePath.getParent();
            FileSystem fs = dirPath.getFileSystem();

            try (WatchService watcher = fs.newWatchService()) {
                watchKey = dirPath.register(watcher, StandardWatchEventKinds.ENTRY_CREATE);

                logger.info("Async Batch Daemon will start watching the creation of a polling stop file. [Path:{}]",
                        filePath);
                detectedWatchKey = detectWatchKey(watcher, watchKey, filePath);
            }

            try {
                threadPoolTaskScheduler.shutdown();
            } catch (Throwable e) {
                logger.error("Async Batch Daemon stopped due to an error. [Error:" + e.getMessage() + "]", e);
            }

            threadPoolTaskExecutor.shutdown();

        } catch (Throwable e) {
            logger.error("Async Batch Daemon stopped due to an error. [Error:" + e.getMessage() + "]", e);
            return FAILURE_STATUS;
        } finally {
            if (detectedWatchKey != null) {
                detectedWatchKey.cancel();
            }
            if (watchKey != null) {
                watchKey.cancel();
            }
        }

        logger.info("Async Batch Daemon stopped after all jobs completed.");
        return 0;
    }

    /**
     * Detect watchKey from WatchService.
     *
     * @param watcher WatchService instance.
     * @param watchKey valid-confirm watchKey
     * @param filePath target file path
     * @return detected watchKey
     * @throws InterruptedException throws if this thread is interrupted in watcher.take().
     */
    private WatchKey detectWatchKey(final WatchService watcher, final WatchKey watchKey, final Path filePath)
            throws InterruptedException {
        WatchKey detectedWatchKey = null;

        while (watchKey.isValid()) {

            detectedWatchKey = watcher.take();

            boolean findTarget = detectedWatchKey.pollEvents().stream()
                    .anyMatch(s -> s.context() != null && isTarget(filePath, (Path) s.context()));
            if (findTarget) {
                logger.info("Async Batch Daemon has detected the polling stop file, and then shutdown now!");
                break;
            }

            detectedWatchKey.reset();
        }
        return detectedWatchKey;
    }

    /**
     * Check whether the file path is correct.
     *
     * @return check result to be an exit code.
     */
    private int checkSpecifiedFilePath() {

        if (!StringUtils.hasText(pollingStopFilePath)) {
            logger.error("Polling stop file is required in application properties. [key:{}]",
                    "async-batch-daemon.polling-stop-file-path");
            return FAILURE_STATUS;
        }

        Path filePath = new File(pollingStopFilePath).toPath();
        if (filePath.toFile().isDirectory()) {
            logger.error("Path is directory. Polling stop file must be a regular file. [Path:{}]", filePath);
            return FAILURE_STATUS;
        } else if (filePath.getParent() == null || !filePath.getParent().toFile().exists()) {
            logger.error("Path not exists. Directory must exist for monitoring file. [Path:{}]", filePath);
            return FAILURE_STATUS;
        } else if (filePath.toFile().exists()) {
            logger.error("Polling stop file already exists. Daemon stop. Please delete the file. [Path:{}]", filePath);
            return FAILURE_STATUS;
        }

        return 0;
    }

    /**
     * Check whether the file matches.
     *
     * @param filePath Comparison source file
     * @param checkPath Files to be compared
     * @return true is match.
     */
    private boolean isTarget(Path filePath, Path checkPath) {
        if (!filePath.getFileName().equals(checkPath.getFileName())) {
            return false;
        }
        if (filePath.toFile().isDirectory()) {
            logger.warn("Polling stop file must be a regular file. [Path:{}]", filePath);
            return false;
        }
        return true;
    }

    /**
     * Static setter for the {@link SystemExiter}.
     *
     * @param systemExiter Implementation of terminating the currently running Java Virtual Machine.
     */
    public static void presetSystemExiter(SystemExiter systemExiter) {
        AsyncBatchDaemon.systemExiter = systemExiter;
    }

    /**
     * Launch a async batch daemon.
     * <p>
     * Launch a job request polling task. Then monitors the file that specify in order to stop the daemon. No exception are
     * thrown from this methods. Instead, exception are logged and an integer is returned through the exit status in
     * {@link SystemExiter}
     * </p>
     * <p>
     * By creating an application context form configLocation, to start a scheduled job request polling task. Next, monitors the
     * file that specify in order to stop the daemon. When it detects that the file has been created the application context
     * closed. As a result, termination process is carried out, which is defined in order to stop the task. Then daemon stopped
     * after all jobs completed.
     * </p>
     * <p>
     * File to be monitored, please set in the key of "async-batch-daemon.polling-stop-file-path" in the properties file
     * </p>
     *
     * @param args <ul>
     *            <li>configLocation: The xml application context containing a customized polling task.</li>
     *            </ul>
     */
    public static void main(String[] args) {

        AsyncBatchDaemon daemon = new AsyncBatchDaemon();

        String configLocation = null;

        if (args.length > 0) {
            configLocation = args[0];
        }

        int result = daemon.start(configLocation);

        systemExiter.exit(result);

    }
}
