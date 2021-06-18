(ns kafka.connect.client.core
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

(defn connector [^KafkaConnectClient client name]
  (.getConnector client name))

(defn add-connector [^KafkaConnectClient client name config]
  (.addConnector client (NewConnectorDefinition. name config)))
