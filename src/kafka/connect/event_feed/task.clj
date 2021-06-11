(ns kafka.connect.event-feed.task
  (:gen-class
   :name io.logicblocks.kafka.connect.eventfeed.SourceTask
   :extends org.apache.kafka.connect.source.SourceTask))

(defn -poll [_]
  [])
