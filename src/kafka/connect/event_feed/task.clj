(ns kafka.connect.event-feed.task
  (:require
   [clojure.tools.logging :as log]

   [kafka.connect.event-feed.logging]
   [kafka.connect.event-feed.utils :as efu]
   [kafka.connect.event-feed.config :as efc]
   [kafka.connect.event-feed.events :as efe]
   [halboy.resource :as hal])
  (:gen-class
   :name io.logicblocks.kafka.connect.eventfeed.EventFeedSourceTask
   :extends org.apache.kafka.connect.source.SourceTask
   :init init
   :state state))

(def default-partition {:partition "default"})

(defn context-offset-map [context]
  (efu/java-data->clojure-data
    (.offset (.offsetStorageReader context)
      (efu/clojure-data->java-data default-partition))))

(defn context-config [context]
  (efc/configuration (.configs context)))

(defn state-atom [this]
  (.state this))

(defn state [this]
  @(state-atom this))

(defn update-state [this f & args]
  (apply swap! (state-atom this) f args))

(defn config [this]
  (:config (state this)))

(defn offset [this]
  (:offset (state this)))

(defn -init []
  [[] (atom nil)])

(defn -initialize [this context]
  (let [offset-map (context-offset-map context)
        offset (:offset offset-map)]
    (log/infof "EventFeedSourceTask[config: %s] initializing with offset: %s..."
      (pr-str (context-config context))
      (or offset "nil"))
    (update-state this assoc :offset offset)))

(defn -start [this props]
  (let [config (efc/configuration props)]
    (log/infof "EventFeedSourceTask[config: %s] starting..."
      (pr-str config))
    (update-state this assoc :config config)))

(defn -stop [this]
  (log/infof "EventFeedSourceTask[config: %s] stopping..."
    (pr-str (config this)))
  (update-state this assoc :config nil))

(defn wait-interval [_]
  (Thread/sleep 200))

(defn fetch-events [this]
  (let [config (config this)
        offset (offset this)]
    (log/infof
      (str "EventFeedSourceTask[name: %s] looking for new events: "
        "[url: %s, per-page: %s, offset: %s]")
      (efc/connector-name config)
      (efc/event-feed-discovery-url config)
      (efc/event-feed-events-per-page config)
      offset)
    (let [events (efe/load-new-events config offset)
          records (efe/events->source-records config events default-partition)
          new-offset (efe/event->offset config (last events))]
      (if (empty? events)
        (do
          (log/debugf "EventFeedSourceTask[name: %s] found no new events."
            (efc/connector-name config))
          [[] nil])
        (do
          (log/debugf "EventFeedSourceTask[name: %s] found new events: %s"
            (efc/connector-name config)
            (pr-str events))
          [records new-offset])))))

(defn commit-offset [this offset]
  (when offset
    (log/debugf
      "EventFeedSourceTask[name: %s] committing offset to memory: %s"
      (efc/connector-name (config this))
      offset)
    (update-state this assoc :offset offset)))

(defn -poll [this]
  (try
    (do
      (wait-interval this)
      (let [[records offset] (fetch-events this)]
        (commit-offset this offset)
        records))
    (catch Throwable t
      (log/errorf
        (str "EventFeedSourceTask[name: %s] encountered exception during"
          " poll: %s")
        (efc/connector-name (config this))
        (Throwable->map t)))))

(defn -commit [this]
  (log/debugf
    "EventFeedSourceTask[name: %s] committing records up to offset: %s"
    (efc/connector-name (config this))
    (offset this)))

(defn -commitRecord [this record metadata]
  (log/debugf "EventFeedSourceTask[name: %s] committing record: %s, %s"
    (efc/connector-name (config this))
    (pr-str record)
    (pr-str metadata)))

(defn -version [_]
  "0.0.1")
