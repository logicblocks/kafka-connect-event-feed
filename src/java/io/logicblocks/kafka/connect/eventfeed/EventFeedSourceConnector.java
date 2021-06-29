package io.logicblocks.kafka.connect.eventfeed;

import clojure.java.api.Clojure;
import clojure.lang.Atom;
import clojure.lang.IFn;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.List;
import java.util.Map;

public class EventFeedSourceConnector extends SourceConnector {
    private static final String CLOJURE_CORE_NS = "clojure.core";
    private static final String CONNECTOR_NS =
            "kafka.connect.event-feed.connector";

    static {
        IFn require = Clojure.var(CLOJURE_CORE_NS, "require");
        require.invoke(Clojure.read(CONNECTOR_NS));
    }

    private final Atom state;

    public EventFeedSourceConnector() {
        super();
        IFn atom = Clojure.var(CLOJURE_CORE_NS, "atom");
        state = (Atom) atom.invoke(null);
    }

    @Override
    public void start(Map<String, String> props) {
        IFn start = Clojure.var(CONNECTOR_NS, "start");
        start.invoke(state, props);
    }

    @Override
    public void stop() {
        IFn start = Clojure.var(CONNECTOR_NS, "stop");
        start.invoke(state);
    }

    @Override
    public ConfigDef config() {
        IFn config = Clojure.var(CONNECTOR_NS, "config");
        return (ConfigDef) config.invoke(state);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<? extends Task> taskClass() {
        IFn taskClass = Clojure.var(CONNECTOR_NS, "task-class");
        return (Class<? extends Task>) taskClass.invoke(state);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        IFn taskConfigs = Clojure.var(CONNECTOR_NS, "task-configs");
        return (List<Map<String, String>>) taskConfigs.invoke(state, maxTasks);
    }

    @Override
    public String version() {
        IFn version = Clojure.var(CONNECTOR_NS, "version");
        return (String) version.invoke(state);
    }
}
