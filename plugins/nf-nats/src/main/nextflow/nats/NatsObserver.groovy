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

import io.nats.client.Connection;
import io.nats.client.Nats;

/**
 * Workflow events Nats publisher
 *
 */
@Slf4j
@CompileStatic
class NatsObserver implements TraceObserver {

    private Connection nc
    private String subject
    private String nats_url

    NatsObserver(String nats_url, String subject) {
        // Connect to NATS
        this.nc = Nats.connect(nats_url)
        this.subject = subject
    }

    void publishEvent(String eventType, String message) {
        String sub_subject = "${subject}.task.events"
        nc.publish(sub_subject, ("[" + eventType + "] " + message).bytes)
    }

     void publishLog(String logType, String message) {
        String sub_subject = "${subject}.task.logs"
        nc.publish(sub_subject, ("[" + logType + "] " + message).bytes)
    }

    @Override
    void onFlowBegin() {
        publishEvent("flow.begin", "Workflow execution started")
    }

    @Override
    void onProcessStart(TaskHandler handler, TraceRecord trace) {
        publishEvent("process.start", "Started task: '${handler.task.name}'")
        publishLogs(handler)
    }

    @Override
    void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        publishEvent("process.complete", "Completed task: '${handler.task.name}'")
        publishLogs(handler)
    }
 
    @Override
    void onFlowError(TaskHandler handler, TraceRecord trace) {
        publishEvent("flow.error", "Workflow encountered an error")
        publishLogs(handler)
    }

    @Override
    void onFlowComplete() {
        publishEvent("flow.complete", "Workflow execution completed")
        nc.close()
    }

    void publishLogs(TaskHandler handler) {
        Path workDir = handler.task.workDir

        try {
            Path stdoutPath = workDir.resolve("stdout")
            Path stderrPath = workDir.resolve("stderr")

            if (Files.exists(stdoutPath)) {
                String stdout = new String(Files.readAllBytes(stdoutPath))
                publishLog("stdout", "Task '${handler.task.name}' stdout: ${stdout}")
            }

            if (Files.exists(stderrPath)) {
                String stderr = new String(Files.readAllBytes(stderrPath))
                publishLog("stderr", "Task '${handler.task.name}' stderr: ${stderr}")
            }
        } catch (IOException e) {
            e.printStackTrace()
        }
    }
}
