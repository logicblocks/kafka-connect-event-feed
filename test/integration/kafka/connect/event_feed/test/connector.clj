(ns kafka.connect.event-feed.test.connector
  (:require
   [kafka.connect.client.core :as kcc]
   [kafka.testing.connect :as ktkc]))

(def connector-class
  "io.logicblocks.kafka.connect.eventfeed.EventFeedSourceConnector")

(defn kafka-connect-client [kafka-connect]
  (let [admin-url (ktkc/admin-url kafka-connect)]
    (kcc/client {:url admin-url})))

(defmacro with-connector [kafka-connect options & body]
  `(let [client# (kafka-connect-client ~kafka-connect)]
     (kcc/add-connector client# (:name ~options) (:config ~options))
     ~@body
     (kcc/delete-connector client# (:name ~options))))
