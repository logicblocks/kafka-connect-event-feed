(ns kafka.connect.event-feed.task
  (:require
   [clojure.core.cache :as cache]
   [clojure.tools.logging :as log]

   [kafka.connect.event-feed.logging]
   [kafka.connect.event-feed.utils :as efu]
   [kafka.connect.event-feed.config :as efc]
   [kafka.connect.event-feed.version :as efv]
   [kafka.connect.event-feed.events :as efe]))

(def default-partition {:partition "default"})

(defn context-offset-map [context]
  (efu/java-data->clojure-data
    (.offset (.offsetStorageReader context)
      (efu/clojure-data->java-data default-partition))))

(defn context-config [context]
  (efc/configuration (.configs context)))

(defn state [state-atom]
  @state-atom)

(defn update-state [state-atom f & args]
  (apply swap! state-atom f args))

(defn config [state-atom]
  (:config (state state-atom)))

(defn offset [state-atom]
  (:offset (state state-atom)))

(defn records-committed [state-atom]
  (:records-committed (state state-atom)))

(def five-minutes-in-ms (* 5 60 1000))
(defn cache [state-atom] (:cache @state-atom))

(defn initialize [state-atom context]
  (let [offset-map (context-offset-map context)
        offset (:offset offset-map)]
    (log/infof "EventFeedSourceTask[config: %s] initializing with offset: %s..."
      (pr-str (context-config context))
      (or offset "nil"))
    (update-state state-atom assoc
      :offset offset)))

(defn start [state-atom props]
  (let [config (efc/configuration props)]
    (log/infof "EventFeedSourceTask[name: %s] has configuration: %s"
      (efc/connector-name config)
      (pr-str config))
    (log/infof "EventFeedSourceTask[name: %s] starting..."
      (efc/connector-name config))
    (update-state state-atom assoc
      :config config
      :records-committed 0
      :cache (atom (cache/ttl-cache-factory {} :ttl five-minutes-in-ms)))))

(defn stop [state-atom]
  (log/infof "EventFeedSourceTask[name: %s] stopping..."
    (efc/connector-name (config state-atom)))
  (update-state state-atom assoc :config nil))

(defn report-commit-count [state-atom]
  (log/debugf
    "EventFeedSourceTask[name: %s] committed %s records to topic since start."
    (efc/connector-name (config state-atom))
    (records-committed state-atom)))

(defn wait-interval [state-atom]
  (let [config (config state-atom)
        interval-ms (efc/polling-fetch-interval-milliseconds config)]
    (log/debugf
      (str "EventFeedSourceTask[name: %s] waiting %s ms before looking for "
        "new events...")
      (efc/connector-name config)
      interval-ms)
    (Thread/sleep interval-ms)))

(defn fetch-events [state-atom]
  (let [config (config state-atom)
        offset (offset state-atom)
        cache (cache state-atom)]
    (log/debugf
      (str "EventFeedSourceTask[name: %s] looking for new events: "
        "[url: %s, per-page: %s, offset: %s]")
      (efc/connector-name config)
      (efc/event-feed-discovery-url config)
      (efc/event-feed-events-per-page config)
      offset)
    (let [events (efe/load-new-events cache config offset)
          records (efe/events->source-records config events default-partition)
          new-offset (efe/event->offset config (last events))]
      (if (empty? events)
        (do
          (log/debugf "EventFeedSourceTask[name: %s] found no new events."
            (efc/connector-name config))
          [[] nil])
        (do
          (log/debugf "EventFeedSourceTask[name: %s] found %s new events."
            (efc/connector-name config)
            (count events))
          (log/debugf "EventFeedSourceTask[name: %s] new events are: %s"
            (efc/connector-name config)
            (pr-str events))
          [records new-offset])))))

(defn commit-offset [state-atom offset]
  (when offset
    (log/debugf
      "EventFeedSourceTask[name: %s] committing offset to memory: %s"
      (efc/connector-name (config state-atom))
      offset)
    (update-state state-atom assoc :offset offset)))

(defn poll [state-atom]
  (try
    (do
      (report-commit-count state-atom)
      (wait-interval state-atom)
      (let [[records offset] (fetch-events state-atom)]
        (commit-offset state-atom offset)
        records))
    (catch Throwable t
      (log/errorf
        (str "EventFeedSourceTask[name: %s] encountered exception during"
          " poll: %s")
        (efc/connector-name (config state-atom))
        (Throwable->map t)))))

(defn commit [state-atom]
  (log/debugf
    "EventFeedSourceTask[name: %s] committed records up to offset: %s"
    (efc/connector-name (config state-atom))
    (offset state-atom)))

(defn commit-record [state-atom record metadata]
  (update-state state-atom
    (fn [state]
      (update-in state [:records-committed] inc)))
  (log/debugf "EventFeedSourceTask[name: %s] committing record: %s, %s"
    (efc/connector-name (config state-atom))
    (pr-str record)
    (pr-str metadata)))

(defn version [_]
  efv/string)
