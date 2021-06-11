(ns kafka-connect-event-feed.core-test
  (:require
   [clojure.test :refer :all])
  (:import
   [org.apache.kafka.connect.source SourceTask]
   [kafka_connect_event_feed EventFeedSourceTask]))

(deftest creates-valid-source-task
  (let [task (EventFeedSourceTask.)]
    (is (instance? SourceTask task))))
