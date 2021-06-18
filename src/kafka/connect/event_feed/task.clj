(ns kafka.connect.event-feed.task
  (:require
   [clojure.tools.logging :as log]

   [halboy.navigator :as halnav]
   [halboy.resource :as hal]
   [halboy.json :as haljson]

   [kafka.connect.event-feed.logging]
   [kafka.connect.event-feed.utils :as efu]
   [kafka.connect.event-feed.records :as efr])
  (:import
   [java.util ArrayList Collection])
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
  (let [state @(.state this)
        topic-name (:topic.name state)
        event-feed-discovery-url (:eventfeed.discovery.url state)
        discovery-navigator (halnav/discover event-feed-discovery-url)
        events-navigator (halnav/get discovery-navigator :events)
        events-resource (halnav/resource events-navigator)
        events (hal/get-resource events-resource :events)

        records (map #(efr/source-record
                        :offset nil
                        :topic-name topic-name
                        :key nil
                        :value (haljson/resource->map %))
                  events)]
    (ArrayList. ^Collection records)))

(defn -version [_]
  "0.0.1")
