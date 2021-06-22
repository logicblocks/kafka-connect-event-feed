(ns kafka.connect.event-feed.records
  (:require
   [kafka.connect.event-feed.utils :as efu])
  (:import
   [org.apache.kafka.connect.source SourceRecord]
   [java.util Collection ArrayList]))

(defn partition-map []
  (efu/clojure-data->java-data
    {:partition "default"}))

(defn offset-map [offset]
  (efu/clojure-data->java-data
    {:offset offset}))

(defn source-record
  [& {:keys [source-partition source-offset topic-name key value]}]
  (SourceRecord.
    (efu/clojure-data->java-data source-partition)
    (efu/clojure-data->java-data source-offset)
    topic-name
    nil
    (efu/clojure-data->java-data key)
    nil
    (efu/clojure-data->java-data value)))

(defn source-records [records]
  (ArrayList. ^Collection records))
