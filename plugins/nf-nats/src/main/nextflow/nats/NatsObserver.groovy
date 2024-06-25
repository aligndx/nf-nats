/*
 * Copyright 2021, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nextflow.nats

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import java.nio.file.Files
import java.nio.file.Path
import nextflow.processor.TaskHandler
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord

import io.nats.client.Connection
import io.nats.client.Nats
import io.nats.client.JetStream

/**
 * Workflow events Nats publisher
 *
 */
@Slf4j
@CompileStatic
class NatsObserver implements TraceObserver {

    private Connection nc
    private JetStream js
    private String subject
    private String natsUrl
    private Set<String> eventsToPublish
    private boolean useJetStream

    NatsObserver(String natsUrl, String subject, Set<String> eventsToPublish, boolean useJetStream) {
        // Connect to NATS
        this.nc = Nats.connect(natsUrl)
        this.subject = subject
        this.eventsToPublish = eventsToPublish
        this.useJetStream = useJetStream

        if (useJetStream) {
            this.js = nc.jetStream()
        }
    }

    void publishEvent(String eventType, String message) {
        if (eventsToPublish.contains(eventType)) {
            String subSubject = "${subject}.events.${eventType}"
            String formattedMessage = createJsonMessage(eventType, message)
            if (useJetStream) {
                js.publish(subSubject, formattedMessage.bytes)
            } else {
                nc.publish(subSubject, formattedMessage.bytes)
            }
        }
    }

    void publishLog(String logType, String message) {
        if (eventsToPublish.contains("process.logs")) {
            String subSubject = "${subject}.logs.${logType}"
            String formattedMessage = createJsonMessage(logType, message)
            if (useJetStream) {
                js.publish(subSubject, formattedMessage.bytes)
            } else {
                nc.publish(subSubject, formattedMessage.bytes)
            }
        }
    }

    private String createJsonMessage(String type, String message) {
        return "{\"type\": \"${type}\", \"message\": \"${message}\"}"
    }

    @Override
    void onFlowBegin() {
        publishEvent("workflow.start", "Workflow execution started")
    }

    @Override
    void onFlowError(TaskHandler handler, TraceRecord trace) {
        publishEvent("workflow.error", "Workflow encountered an error")
        publishLogs(handler)
    }

    @Override
    void onFlowComplete() {
        publishEvent("workflow.complete", "Workflow execution completed")
        nc.close()
    }

    @Override
    void onProcessStart(TaskHandler handler, TraceRecord trace) {
        publishEvent("process.start", "Started task: '${handler.task.name}'")
    }

    @Override
    void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        publishEvent("process.complete", "Completed task: '${handler.task.name}'")
        publishLogs(handler)
    }

    void publishLogs(TaskHandler handler) {
        Path workDir = handler.task.workDir

        if (workDir == null) {
            log.warn("Task '${handler.task.name}' workDir is null")
            return
        }

        try {
            log.debug("Checking logs for task: '${handler.task.name}' in directory: ${workDir}")

            Path commandOutPath = workDir.resolve(".command.out")
            Path commandErrPath = workDir.resolve(".command.err")

            if (Files.exists(commandOutPath)) {
                log.debug("Found .command.out for task: '${handler.task.name}'")
                String commandOut = new String(Files.readAllBytes(commandOutPath))
                publishLog("logs.stdout", "Task '${handler.task.name}' : ${commandOut}")
            } else {
                log.debug("No .command.out found for task: '${handler.task.name}'")
            }

            if (Files.exists(commandErrPath)) {
                log.debug("Found .command.err for task: '${handler.task.name}'")
                String commandErr = new String(Files.readAllBytes(commandErrPath))
                publishLog("logs.stderr", "Task '${handler.task.name}' : ${commandErr}")
            } else {
                log.debug("No .command.err found for task: '${handler.task.name}'")
            }
        } catch (IOException e) {
            log.error("Error reading log files for task '${handler.task.name}'", e)
        }
    }
}
