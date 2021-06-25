(ns kafka.connect.event-feed.test.connector
  (:require
   [kafka.connect.client.core :as kcc]
   [kafka.testing.connect :as ktkc]))

(def connector-class
  "io.logicblocks.kafka.connect.eventfeed.EventFeedSourceConnector")

(defmacro with-connector [kafka-connect options & body]
  `(let [admin-url# (ktkc/admin-url ~kafka-connect)
         client# (kcc/client {:url admin-url#})]
     (kcc/add-connector client# (:name ~options) (:config ~options))
     ~@body))
