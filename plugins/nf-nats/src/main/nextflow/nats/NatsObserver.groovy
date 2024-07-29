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
        Map<String, Object> messageMap = [
            timestamp: DateTimeFormatter.ISO_INSTANT.format(Instant.now()),
            type     : type,
            message  : message
        ]

        if (trace != null) {
            Object startObj = trace.get('start')
            Object completeObj = trace.get('complete')

            String formattedStart = startObj ? DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(startObj as Long)) : ''
            String formattedComplete = completeObj ? DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(completeObj as Long)) : ''

            messageMap.task = [
                id       : trace.get('task_id'),
                name     : trace.get('name'),
                status   : trace.get('status'),
                start    : formattedStart,
                complete : formattedComplete,
                realtime : "${trace.get('realtime')}ms"
            ]
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
        publishLogs(handler, trace)
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
