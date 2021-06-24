(ns kafka.connect.event-feed.task
  (:require
   [clojure.tools.logging :as log]

   [kafka.connect.event-feed.logging]
   [kafka.connect.event-feed.utils :as efu]
   [kafka.connect.event-feed.events :as efe]
   [halboy.resource :as hal])
  (:gen-class
   :name io.logicblocks.kafka.connect.eventfeed.EventFeedSourceTask
   :extends org.apache.kafka.connect.source.SourceTask
   :init init
   :state state))

(def default-partition {:partition "default"})

(defn -init []
  [[] (atom nil)])

(defn -initialize [this context]
  (let [state-atom (.state this)
        offset-map
        (efu/java-data->clojure-data
          (.offset (.offsetStorageReader context)
            (efu/clojure-data->java-data default-partition)))
        offset (:offset offset-map)]
    (swap! state-atom assoc :offset offset)))

(defn -start [this props]
  (let [state-atom (.state this)
        config (efu/java-data->clojure-data props)]
    (log/infof "EventFeedSourceTask[config: %s] starting..."
      (pr-str config))
    (swap! state-atom assoc :config config)))

(defn -stop [this]
  (let [state-atom (.state this)
        config (deref state-atom)]
    (log/infof "EventFeedSourceTask[config: %s] stopping..."
      (pr-str config))
    (swap! state-atom assoc :config nil)))

(defn wait-interval [_]
  (Thread/sleep 200))

(defn config [this]
  (:config @(.state this)))

(defn offset [this]
  (:offset @(.state this)))

(defn commit-offset [this offset]
  (when offset
    (let [state-atom (.state this)]
      (swap! state-atom assoc :offset offset))))

(defn -poll [this]
  (try
    (let [config (config this)
          offset (offset this)]
      (wait-interval config)
      (log/infof
        (str "EventFeedSourceTask[name: %s] loading new events: "
          "[url: %s, pick: %s, since: %s]")
        (:name config)
        (:eventfeed.discovery.url config)
        (:eventfeed.pick config)
        offset)
      (let [events (efe/load-new-events config offset)
            records (efe/events->source-records config events default-partition)
            new-offset (hal/get-property (last events) :id)]
        (log/debugf "EventFeedSourceTask[name: %s] found events: %s"
          (:name config)
          (pr-str events))
        (log/debugf
          "EventFeedSourceTask[name: %s] committing offset to memory: %s"
          (:name config)
          new-offset)
        (commit-offset this new-offset)
        records))
    (catch Throwable t
      (log/errorf
        (str "EventFeedSourceTask[name: %s] encountered exception during"
          " poll: %s")
        (:name (config this))
        (Throwable->map t)))))

(defn -commit [this]
  (log/debugf
    "EventFeedSourceTask[name: %s] committing records up to offset: %s"
    (:name (config this))
    (offset this)))

(defn -commitRecord [this record metadata]
  (log/debugf "EventFeedSourceTask[name: %s] committing record: %s, %s"
    (:name (config this))
    (pr-str record)
    (pr-str metadata)))

(defn -version [_]
  "0.0.1")
