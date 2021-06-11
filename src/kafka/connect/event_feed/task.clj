(ns kafka-connect-event-feed.task
  (:gen-class
   :name kafka_connect_event_feed.EventFeedSourceTask
   :extends org.apache.kafka.connect.source.SourceTask))

(defn -poll [_]
  [])
