(ns kafka.connect.event-feed.connector
  (:require
   [clojure.tools.logging :as log]

   [kafka.connect.event-feed.task]
   [kafka.connect.event-feed.logging]
   [kafka.connect.event-feed.config :as efc]
   [kafka.connect.event-feed.version :as efv])
  (:import
   [io.logicblocks.kafka.connect.eventfeed
    EventFeedSourceTask]))

(defn start [state-atom props]
  (let [config (efc/configuration props)]
    (log/infof "EventFeedSourceConnector[name: %s] has configuration: %s"
      (efc/connector-name config)
      (pr-str config))
    (log/infof "EventFeedSourceConnector[name: %s] starting..."
      (efc/connector-name config))
    (reset! state-atom
      {:config config
       :properties props})))

(defn stop [state-atom]
  (let [state (deref state-atom)
        config (:config state)]
    (log/infof "EventFeedSourceConnector[name: %s] stopping..."
      (efc/connector-name config))
    (reset! state-atom nil)))

(defn config [_]
  (efc/configuration-definition))

(defn version [_]
  (efv/string))

(defn task-class [_]
  EventFeedSourceTask)

(defn task-configs [state-atom max-tasks]
  (let [state (deref state-atom)
        props (:properties state)]
    (repeat max-tasks props)))
