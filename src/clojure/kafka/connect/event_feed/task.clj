(ns kafka.connect.event-feed.task
  (:require
   [clojure.tools.logging :as log]

   [kafka.connect.event-feed.logging]
   [kafka.connect.event-feed.utils :as efu]
   [kafka.connect.event-feed.config :as efc]
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

(defn initialize [state-atom context]
  (let [offset-map (context-offset-map context)
        offset (:offset offset-map)]
    (log/infof "EventFeedSourceTask[config: %s] initializing with offset: %s..."
      (pr-str (context-config context))
      (or offset "nil"))
    (update-state state-atom assoc :offset offset)))

(defn start [state-atom props]
  (let [config (efc/configuration props)]
    (log/infof "EventFeedSourceTask[config: %s] starting..."
      (pr-str config))
    (update-state state-atom assoc :config config)))

(defn stop [state-atom]
  (log/infof "EventFeedSourceTask[config: %s] stopping..."
    (pr-str (config state-atom)))
  (update-state state-atom assoc :config nil))

(defn wait-interval [_]
  (Thread/sleep 200))

(defn fetch-events [state-atom]
  (let [config (config state-atom)
        offset (offset state-atom)]
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
    "EventFeedSourceTask[name: %s] committing records up to offset: %s"
    (efc/connector-name (config state-atom))
    (offset state-atom)))

(defn commit-record [state-atom record metadata]
  (log/debugf "EventFeedSourceTask[name: %s] committing record: %s, %s"
    (efc/connector-name (config state-atom))
    (pr-str record)
    (pr-str metadata)))

(defn version [_]
  "0.0.1")
