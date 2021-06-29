(ns kafka.connect.event-feed.records
  (:refer-clojure :exclude [key])
  (:require
   [kafka.connect.event-feed.utils :as efu])
  (:import
   [java.util ArrayList Collection]
   [org.apache.kafka.connect.source SourceRecord]))

(defn source-record
  [& {:keys [source-partition
             source-offset
             topic-name
             key
             value]}]
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
