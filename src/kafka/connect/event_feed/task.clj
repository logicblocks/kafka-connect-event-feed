(ns kafka.connect.event-feed.task
  (:require
    [clojure.tools.logging :as log]

    [kafka.connect.event-feed.logging]
    [kafka.connect.event-feed.utils :as efu]
    [kafka.connect.event-feed.events :as efe])
  (:gen-class
    :name io.logicblocks.kafka.connect.eventfeed.EventFeedSourceTask
    :extends org.apache.kafka.connect.source.SourceTask
    :init init
    :state state))

(defn -init []
  [[] (atom nil)])

(defn -start [this props]
  (let [state-atom (.state this)
        config (efu/java-data->clojure-data props)]
    (log/infof "Starting EventFeedSourceTask [config: %s]"
      (pr-str config))
    (reset! state-atom config)))

(defn -stop [this]
  (let [state-atom (.state this)
        config (deref state-atom)]
    (log/infof "Stopping EventFeedSourceTask [config: %s]"
      (pr-str config))
    (reset! state-atom nil)))

(defn wait-interval [_]
  (Thread/sleep 200))

(defn -poll [this]
  (let [config @(.state this)]
    (wait-interval config)
    (efe/events->source-records config
      (efe/load-new-events config))))

(defn -version [_]
  "0.0.1")
