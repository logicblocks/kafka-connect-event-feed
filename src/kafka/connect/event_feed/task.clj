(ns kafka.connect.event-feed.task
  (:require
   [clojure.tools.logging :as log]

   [kafka.connect.event-feed.logging]
   [kafka.connect.event-feed.utils :as efu]
   [kafka.connect.event-feed.records :as efr])
  (:import
   [java.util ArrayList Collection UUID])
  (:gen-class
   :name io.logicblocks.kafka.connect.eventfeed.EventFeedSourceTask
   :extends org.apache.kafka.connect.source.SourceTask
   :init init
   :state state))

(defn -init []
  [[] (atom nil)])

(defn -start [this props]
  (let [state-atom (.state this)
        config (efu/property-map->clojure-map props)]
    (log/infof "Starting EventFeedSourceTask [config: %s]"
      (pr-str config))
    (reset! state-atom config)))

(defn -stop [this]
  (let [state-atom (.state this)
        config (deref state-atom)]
    (log/infof "Stopping EventFeedSourceTask [config: %s]"
      (pr-str config))
    (reset! state-atom nil)))

(defn -poll [this]
  (Thread/sleep 200)
  (let [id (str (UUID/randomUUID))
        state @(.state this)
        topic-name (:topic.name state)
        record (efr/source-record
                 :offset id
                 :topic-name topic-name
                 :key id
                 :value (efu/clojure-data->java-data state))]
    (ArrayList. ^Collection [record])))

(defn -version [_]
  "0.0.1")
