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
import nextflow.Session
import nextflow.trace.TraceObserver
import nextflow.trace.TraceObserverFactory
/**
 * Factory logic for Nats Observer
 *
 */
@CompileStatic
class NatsFactory implements TraceObserverFactory {

    @Override
    Collection<TraceObserver> create(Session session) {
        def isEnabled = session.config.navigate('nats.enabled') as Boolean
        def url = session.config.navigate('nats.url') as String
        def subject = session.config.navigate('nats.subject') as String
        def events = session.config.navigate('nats.events') as List<String> ?: ['workflow.start', 'workflow.error', 'workflow.complete', 'process.start', 'process.complete', 'process.logs']
        def jetstream = session.config.navigate('nats.jetstream') as Boolean ?: false

        def result = new ArrayList()
        if ( isEnabled ) {
            def observer = new NatsObserver(url, subject, events.toSet(), jetstream)
            result << observer
        }
        return result
    }
}
