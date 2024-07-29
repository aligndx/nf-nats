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
        this.natsUrl = natsUrl
        this.subject = subject
        this.eventsToPublish = eventsToPublish
        this.useJetStream = useJetStream
        connectToNats()
    }

    private void connectToNats() {
        nc = Nats.connect(natsUrl)
        if (useJetStream) {
            js = nc.jetStream()
        }
    }
    
    void publishEvent(String eventType, String message) {
        if (eventsToPublish.contains(eventType)) {
            String subSubject = "${subject}.events.${eventType}"
            String formattedMessage = createJsonMessage(eventType, message)
            sendMessage(subSubject, formattedMessage)
        }
    } 

    void publishLog(String logType, String message) {
        if (eventsToPublish.contains("process.logs")) {
            String subSubject = "${subject}.logs.${logType}"
            String formattedMessage = createJsonMessage(logType, message)
            sendMessage(subSubject, formattedMessage)
        }
    }

    private void sendMessage(String subSubject, String message) {
        if (useJetStream) {
            js.publish(subSubject, message.bytes)
        } else {
            nc.publish(subSubject, message.bytes)
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

        processLogFiles(handler, workDir, ".command.out", "stdout")
        processLogFiles(handler, workDir, ".command.err", "stderr")
    }

    private void processLogFiles(TaskHandler handler, Path workDir, String fileName, String logType) {
        Path filePath = workDir.resolve(fileName)
        if (Files.exists(filePath)) {
            try {
                String content = new String(Files.readAllBytes(filePath))
                publishLog(logType, "Task '${handler.task.name}': ${content}")
            } catch (IOException e) {
                log.error("Error reading file ${fileName} for task '${handler.task.name}'", e)
            }
        } else {
            log.debug("No ${fileName} found for task: '${handler.task.name}'")
        }
    }
}
