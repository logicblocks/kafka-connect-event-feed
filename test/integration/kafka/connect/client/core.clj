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
    NewConnectorDefinition Task Task$TaskId]))

(defn clojure-value->property-value [x]
  (cond
    (keyword? x) (name x)
    :else (str x)))

(defn clojure-map->property-map [clojure-map]
  (HashMap.
    (let [f (fn [[k v]]
              [(name k) (clojure-value->property-value v)])]
      ^Map (w/postwalk
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

(defn connector-tasks
  [^KafkaConnectClient client connector-name]
  (.getConnectorTasks client (name connector-name)))

(defn restart-connector-task
  [^KafkaConnectClient client connector-name task]
  (.restartConnectorTask
    client
    (name connector-name)
    (-> task (.getId) (.getTask))))

(defn restart-connector-tasks
  [^KafkaConnectClient client connector-name]
  (let [tasks (connector-tasks client connector-name)]
    (doseq [^Task task tasks]
          (restart-connector-task client connector-name task))))

(defn add-connector [^KafkaConnectClient client connector-name config]
  (.addConnector client
    (NewConnectorDefinition.
      (name connector-name)
      (clojure-map->property-map config))))

(defn delete-connector [^KafkaConnectClient client connector-name]
  (.deleteConnector client (name connector-name)))

(defn restart-connector
  ([^KafkaConnectClient client connector-name]
   (restart-connector client connector-name {}))
  ([^KafkaConnectClient client connector-name opts]
   (.restartConnector client (name connector-name))
   (when (get opts :include-tasks?)
     (restart-connector-tasks client connector-name))))
