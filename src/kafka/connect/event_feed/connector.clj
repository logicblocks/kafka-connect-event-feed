(ns kafka.connect.event-feed.connector
  (:require
   [clojure.tools.logging :as log]

   [kafka.connect.event-feed.task]
   [kafka.connect.event-feed.logging]
   [kafka.connect.event-feed.utils :as efu]
   [kafka.connect.event-feed.config :as efc])
  (:import
   [io.logicblocks.kafka.connect.eventfeed
    EventFeedSourceTask])
  (:gen-class
   :name io.logicblocks.kafka.connect.eventfeed.EventFeedSourceConnector
   :extends org.apache.kafka.connect.source.SourceConnector
   :init init
   :state state))

(defn -init []
  [[] (atom nil)])

(defn -start [this props]
  (let [state-atom (.state this)
        config (efc/configuration props)]
    (log/infof "EventFeedSourceConnector[config: %s] starting..."
      (pr-str config))
    (reset! state-atom
      {:config config
       :properties props})))

(defn -stop [this]
  (let [state-atom (.state this)
        state (deref state-atom)
        config (:config state)]
    (log/infof "EventFeedSourceConnector[config: %s] stopping..."
      (pr-str config))
    (reset! state-atom nil)))

(defn -config [_]
  (efc/configuration-definition))

(defn -version [_]
  "0.0.1")

(defn -taskClass [_]
  EventFeedSourceTask)

(defn -taskConfigs [this max-tasks]
  (let [state-atom (.state this)
        state (deref state-atom)
        props (:properties state)]
    (repeat max-tasks props)))
