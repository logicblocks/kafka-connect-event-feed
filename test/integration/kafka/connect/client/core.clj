(ns kafka.connect.client.core
  (:require
    [clojure.walk :as w]

    [kafka.connect.event-feed.utils :as efu])
  (:import
    [java.util HashMap Map]
    [org.sourcelab.kafka.connect.apiclient
     Configuration
     KafkaConnectClient]
    [org.sourcelab.kafka.connect.apiclient.request.dto
     NewConnectorDefinition]))

(defn clojure-value->property-value [x]
  (cond
    (keyword? x) (name x)
    :else (str x)))

(defn clojure-map->property-map [clojure-map]
  (HashMap. ^Map
    (let [f (fn [[k v]]
              [(name k) (clojure-value->property-value v)])]
      (w/postwalk
        (fn [x] (if (map? x) (into {} (map f x)) x))
        clojure-map))))

(defn- configuration [{:keys [url]}]
  (Configuration. url))

(defn client [options]
  (KafkaConnectClient. (configuration options)))

(defn connectors [^KafkaConnectClient client]
  (.getConnectors client))

(defn connector [^KafkaConnectClient client connector-name]
  (.getConnector client connector-name))

(defn add-connector [^KafkaConnectClient client connector-name config]
  (.addConnector client
    (NewConnectorDefinition.
      (name connector-name)
      (clojure-map->property-map config))))
