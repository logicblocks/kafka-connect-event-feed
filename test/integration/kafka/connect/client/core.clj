(ns kafka.connect.client.core
  (:require
   [kafka.connect.event-feed.utils :as efu])
  (:import
   [org.sourcelab.kafka.connect.apiclient
    Configuration
    KafkaConnectClient]
   [org.sourcelab.kafka.connect.apiclient.request.dto NewConnectorDefinition]))

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
      (efu/clojure-data->java-data config))))
