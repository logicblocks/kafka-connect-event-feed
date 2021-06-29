package io.logicblocks.kafka.connect.eventfeed;

import clojure.java.api.Clojure;
import clojure.lang.Atom;
import clojure.lang.IFn;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;

public class EventFeedSourceTask extends SourceTask {
    private static final String CLOJURE_CORE_NS = "clojure.core";
    private static final String TASK_NS =
            "kafka.connect.event-feed.task";

    static {
        IFn require = Clojure.var(CLOJURE_CORE_NS, "require");
        require.invoke(Clojure.read(TASK_NS));
    }

    private final Atom state;

    public EventFeedSourceTask() {
        super();
        IFn atom = Clojure.var(CLOJURE_CORE_NS, "atom");
        state = (Atom) atom.invoke(null);
    }

    @Override
    public void start(Map<String, String> props) {
        IFn start = Clojure.var(TASK_NS, "start");
        start.invoke(state, props);
    }

    @Override
    public void stop() {
        IFn start = Clojure.var(TASK_NS, "stop");
        start.invoke(state);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<SourceRecord> poll() {
        IFn poll = Clojure.var(TASK_NS, "poll");
        return (List<SourceRecord>) poll.invoke(state);
    }

    @Override
    public void commit() throws InterruptedException {
        IFn commit = Clojure.var(TASK_NS, "commit");
        commit.invoke(state);
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        IFn commitRecord = Clojure.var(TASK_NS, "commit-record");
        commitRecord.invoke(state, record, metadata);
    }

    @Override
    public String version() {
        IFn version = Clojure.var(TASK_NS, "version");
        return (String) version.invoke(state);
    }
}
