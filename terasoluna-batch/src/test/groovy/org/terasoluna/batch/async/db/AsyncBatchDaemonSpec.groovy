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

import org.springframework.batch.core.launch.support.SystemExiter
import org.springframework.util.ClassUtils
import spock.lang.Narrative
import spock.lang.Specification
import uk.org.lidalia.slf4jext.Level
import uk.org.lidalia.slf4jtest.LoggingEvent
import uk.org.lidalia.slf4jtest.TestLoggerFactory

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

import static org.hamcrest.CoreMatchers.hasItem
import static org.hamcrest.CoreMatchers.hasItems
import static spock.util.matcher.HamcrestSupport.that


/**
 * Test AsyncBatchDaemon
 *
 * @since 5.0.0
 */
@Narrative("""
To start the asynchronous batch daemon Bean definitions that are specified in the default Bean definition or argument.
It detects that the monitored file specified in the properties file is created to stop the asynchronous batch daemon.
""")
class AsyncBatchDaemonSpec extends Specification {

    def systemExiter = Mock(SystemExiter)

    def logger = TestLoggerFactory.getTestLogger(AsyncBatchDaemon.class)


    def cleanup() {
        TestLoggerFactory.clearAll()
        Path path = new File("./watching").toPath()
        Files.deleteIfExists(path)
    }

    def "Load default bean definition"() {

        setup:
        AsyncBatchDaemon.presetSystemExiter(systemExiter);
        def checkLog = LoggingEvent.error("Polling stop file is required in application properties. [key:{}]",
                "async-batch-daemon.polling-stop-file-path")
        when:
        AsyncBatchDaemon.main([] as String[])

        then:
        1 * systemExiter.exit(255)
        that logger.getLoggingEvents(), hasItem(checkLog)
    }


    def "Load bean definition specified in the argument"() {

        setup:
        AsyncBatchDaemon.presetSystemExiter(systemExiter);
        def configLocation = ClassUtils.addResourcePathToPackagePath(AsyncBatchDaemonSpec.class,
                "invalid-path-async-batch-daemon.xml")
        def checkLog = LoggingEvent.error("Path not exists. Directory must exist for monitoring file. [Path:{}]",
                new File("Invalid-Path").toPath())
        when:
        AsyncBatchDaemon.main(configLocation)

        then:
        1 * systemExiter.exit(255)
        that logger.getLoggingEvents(), hasItem(checkLog)
    }

    def "Load non-existent bean definition specified in the argument"() {

        setup:
        AsyncBatchDaemon.presetSystemExiter(systemExiter);
        def configLocation = ClassUtils.addResourcePathToPackagePath(AsyncBatchDaemonSpec.class,
                "non-existent-async-batch-daemon.xml")
        logger.setEnabledLevels(Level.ERROR)

        when:
        AsyncBatchDaemon.main(configLocation)

        then:
        1 * systemExiter.exit(255)
        logger.getLoggingEvents().size() == 1
        logger.getLoggingEvents().get(0).message.startsWith("Async Batch Daemon stopped due to an error.")
    }


    def "Check the non-existent monitoring directory"() {
        setup:
        def configLocation = ClassUtils.addResourcePathToPackagePath(AsyncBatchDaemonSpec.class,
                "not-exist-directory-async-batch-daemon.xml")
        def checkLog = LoggingEvent.error("Path not exists. Directory must exist for monitoring file. [Path:{}]",
                new File("./Unknown/end.txt").toPath())
        def daemon = new AsyncBatchDaemon()

        when:
        def result = daemon.start(configLocation)
        then:
        result == 255
        that logger.getLoggingEvents(), hasItem(checkLog)
    }

    def "Check that the monitoring file is a regular file"() {
        setup:
        def configLocation = ClassUtils.addResourcePathToPackagePath(AsyncBatchDaemonSpec.class,
                "no-regular-file-async-batch-daemon.xml")
        def path = new File("./subdir").toPath()
        Files.deleteIfExists(path)
        Files.createDirectories(path)

        def checkLog = LoggingEvent.error("Path is directory. Polling stop file must be a regular file. [Path:{}]", path)
        def daemon = new AsyncBatchDaemon()

        when:
        def result = daemon.start(configLocation)
        then:
        result == 255
        that logger.getLoggingEvents(), hasItem(checkLog)

        cleanup:
        Files.delete(path)

    }

    def "Only, the daemon is stopped when the monitoring file of the target has been created"() {
        setup:
        def executor = Executors.newSingleThreadExecutor()
        def configLocation = ClassUtils.addResourcePathToPackagePath(AsyncBatchDaemonSpec.class,
                "normal-async-batch-daemon.xml")
        def targetPath = new File("./watching/end.txt").toPath()
        def nonTargetPath = new File("./watching/continue.txt").toPath()
        Files.deleteIfExists(targetPath)
        Files.deleteIfExists(nonTargetPath)
        if (!Files.exists(targetPath.getParent())) {
            Files.createDirectories(targetPath.getParent())
        }
        def daemon = new AsyncBatchDaemon()
        def daemonExecutor = new DaemonExecutor(daemon, configLocation)
        def result = -1
        def checkLogs = []
        checkLogs << LoggingEvent.info("Async Batch Daemon start.")
        checkLogs << LoggingEvent.info("Async Batch Daemon will start watching the creation of a polling stop file. [Path:{}]", targetPath)
        checkLogs << LoggingEvent.info("Async Batch Daemon has detected the polling stop file, and then shutdown now!")
        checkLogs << LoggingEvent.info("Async Batch Daemon stopped after all jobs completed.")
        def find = false

        when:
        def future = executor.submit(daemonExecutor)

        // wait
        for(i in 1..5) {
            find = logger.allLoggingEvents.message.find {
                it == "Async Batch Daemon will start watching the creation of a polling stop file. [Path:{}]"
            } != null

            if (find) {
                break
            }
            sleep(1000)
        }
        if (!find) {
            throw new RuntimeException("retry over")
        }


        try {
            // not target file create
            Files.createFile(nonTargetPath)
            future.get(3L, TimeUnit.SECONDS)
        } catch (TimeoutException ignore) {
            // target file create
            Files.createFile(targetPath)
            result = future.get()
        } finally {
            executor.shutdown()
        }

        then:
        result == 0
        that logger.getAllLoggingEvents(), hasItems(checkLogs as LoggingEvent[])

        cleanup:
        Files.deleteIfExists(targetPath)
        Files.deleteIfExists(nonTargetPath)
    }

    def "the daemon is not stopped when the monitoring file of the target has been created as directory"() {
        setup:
        def executor = Executors.newSingleThreadExecutor()
        def configLocation = ClassUtils.addResourcePathToPackagePath(AsyncBatchDaemonSpec.class,
                "create-no-regular-file-async-batch-daemon.xml")
        def targetPath = new File("./watching/dir").toPath()
        Files.deleteIfExists(targetPath)
        if (!Files.exists(targetPath.getParent())) {
            Files.createDirectories(targetPath.getParent())
        }
        def daemon = new AsyncBatchDaemon()
        def daemonExecutor = new DaemonExecutor(daemon, configLocation)
        def result = -1
        def checkLogs = []
        checkLogs << LoggingEvent.warn("Polling stop file must be a regular file. [Path:{}]", targetPath)
        def find = false

        when:
        def future = executor.submit(daemonExecutor)

        // wait
        for(i in 1..5) {
            find = logger.allLoggingEvents.message.find {
                it == "Async Batch Daemon will start watching the creation of a polling stop file. [Path:{}]"
            } != null

            if (find) {
                break
            }
            sleep(1000)
        }
        if (!find) {
            throw new RuntimeException("retry over")
        }

        try {
            // not target file create
            Files.createDirectories(targetPath)
            result = future.get(10L, TimeUnit.SECONDS)
        } catch (TimeoutException ignore) {
            // target file create
            targetPath.toFile().deleteDir()
            Files.createFile(targetPath)
            result = future.get()
        } finally {
            executor.shutdown()
        }

        then:
        result == 0
        that logger.getAllLoggingEvents(), hasItems(checkLogs as LoggingEvent[])

        cleanup:
        Files.deleteIfExists(targetPath)
    }

    def "If you before daemon startup there is a file to be monitored, to continue to ignore the processing"() {
        setup:
        def executor = Executors.newSingleThreadExecutor()
        def configLocation = ClassUtils.addResourcePathToPackagePath(AsyncBatchDaemonSpec.class,
                "normal-async-batch-daemon.xml")
        def targetPath = new File("./watching/end.txt").toPath()
        Files.deleteIfExists(targetPath)
        if (!Files.exists(targetPath.getParent())) {
            Files.createDirectories(targetPath.getParent())
        }
        Files.createFile(targetPath)
        def daemon = new AsyncBatchDaemon()
        def daemonExecutor = new DaemonExecutor(daemon, configLocation)
        def result = -1
        def checkLog = LoggingEvent.error("Polling stop file already exists. Daemon stop. Please delete the file. [Path:{}]", targetPath)
        when:
        def future = executor.submit(daemonExecutor)

        try {
            result = future.get()
        } finally {
            executor.shutdown()
        }

        then:
        result == 255
        that logger.getAllLoggingEvents(), hasItem(checkLog)

        cleanup:
        Files.deleteIfExists(targetPath)
    }

    def "Verify that the async profile is active"() {
        setup:
        def configLocation = ClassUtils.addResourcePathToPackagePath(AsyncBatchDaemonSpec.class,
                "active-profile-async-batch-daemon.xml")
        def checkLog = LoggingEvent.error("Polling stop file is required in application properties. [key:{}]",
                "async-batch-daemon.polling-stop-file-path")
        def daemon = new AsyncBatchDaemon()

        when:
        def result = daemon.start(configLocation)

        then:
        result == 255
        that logger.getLoggingEvents(), hasItem(checkLog)
    }


    class DaemonExecutor implements Callable<Integer> {

        AsyncBatchDaemon daemon;

        String configLocation;

        DaemonExecutor(AsyncBatchDaemon daemon, String configLocation) {
            this.daemon = daemon
            this.configLocation = configLocation
        }

        @Override
        Integer call() throws Exception {
            return daemon.start(configLocation)
        }
    }

}
