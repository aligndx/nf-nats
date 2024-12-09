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
import java.time.format.DateTimeFormatter
import java.time.Instant

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

    void publishEvent(String eventType, String message, TraceRecord trace = null) {
        if (eventsToPublish.contains(eventType)) {
            String subSubject = "${subject}.events.${eventType}"
            String formattedMessage = createJsonMessage(eventType, message, trace)
            sendMessage(subSubject, formattedMessage)
        }
    }

    void publishLog(String logType, String message, TraceRecord trace) {
        if (eventsToPublish.contains('process.logs')) {
            String subSubject = "${subject}.logs.${logType}"
            String formattedMessage = createJsonMessage(logType, message, trace)
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

    private String createJsonMessage(String type, String message, TraceRecord trace = null) {
        Map<String, Object> messageMap = [:]
        messageMap.put('timestamp', DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
        messageMap.put('type', type)
        messageMap.put('message', message)

        if (trace != null) {
            // Create metadata map with non-null fields
            Map<String, Object> metadata = [:]
            Object startObj = trace.get('start')
            Object completeObj = trace.get('complete')

            if (trace.get('task_id') != null) metadata.put('id', trace.get('task_id'))
            if (trace.get('name') != null) metadata.put('name', trace.get('name'))
            if (trace.get('status') != null) metadata.put('status', trace.get('status'))
            if (startObj != null) metadata.put('start', DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(startObj as Long)))
            if (completeObj != null) metadata.put('complete', DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(completeObj as Long)))
            if (trace.get('realtime') != null) metadata.put('realtime', trace.get('realtime'))

            // Add metadata to message map if it's not empty
            if (!metadata.isEmpty()) {
                messageMap.put('metadata', metadata)
            }
        }

        return new groovy.json.JsonBuilder(messageMap).toString()
    }

    @Override
    void onFlowBegin() {
        publishEvent('workflow.start', 'Workflow execution started')
    }

    @Override
    void onFlowError(TaskHandler handler, TraceRecord trace) {
        publishEvent('workflow.error', 'Workflow encountered an error', trace)

        // Check if handler is not null before proceeding
        if (handler != null) {
            publishLogs(handler, trace)
        } else {
            log.warn('TaskHandler is null during onFlowError')
        }
        nc.close()
    }

    @Override
    void onFlowComplete() {
        publishEvent('workflow.complete', 'Workflow execution completed')
        nc.close()
    }

    @Override
    void onProcessStart(TaskHandler handler, TraceRecord trace) {
        publishEvent('process.start', 'Started task', trace)
    }

    @Override
    void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        publishEvent('process.complete', 'Completed task', trace)
        publishLogs(handler, trace)
    }

    void publishLogs(TaskHandler handler, TraceRecord trace) {
        // Check if handler or handler.task is null
        if (handler == null || handler.task == null) {
            log.warn('Cannot publish logs because TaskHandler or Task is null')
            return
        }

        Path workDir = handler.task.workDir
        if (workDir == null) {
            log.warn("Task '${handler.task.name}' workDir is null")
            return
        }

        processLogFiles(handler, trace, workDir, '.command.out', 'stdout')
        processLogFiles(handler, trace, workDir, '.command.err', 'stderr')
    }

    private void processLogFiles(TaskHandler handler, TraceRecord trace, Path workDir, String fileName, String logType) {
        Path filePath = workDir.resolve(fileName)
        if (Files.exists(filePath)) {
            try {
                String content = new String(Files.readAllBytes(filePath))
                publishLog(logType, content, trace)
            } catch (IOException e) {
                log.error("Error reading file ${fileName} for task '${handler.task.name}'", e)
            }
        } else {
            log.debug("No ${fileName} found for task: '${handler.task.name}'")
        }
    }

}
