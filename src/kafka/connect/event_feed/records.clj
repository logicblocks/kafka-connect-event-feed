(ns kafka.connect.event-feed.records
  (:require
   [kafka.connect.event-feed.utils :as efu])
  (:import
   [org.apache.kafka.connect.source SourceRecord]))

(defn partition-map []
  (efu/clojure-data->java-data
    {:partition "default"}))

(defn offset-map [offset]
  (efu/clojure-data->java-data
    {:offset offset}))

(defn source-record [& {:keys [offset topic-name key value]}]
  (SourceRecord.
    (partition-map)
    (offset-map offset)
    topic-name
    nil
    (efu/clojure-data->java-data key)
    nil
    (efu/clojure-data->java-data value)))
