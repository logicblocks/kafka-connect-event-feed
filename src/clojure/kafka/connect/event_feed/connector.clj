(ns kafka.connect.event-feed.connector
  (:require
   [clojure.tools.logging :as log]

   [kafka.connect.event-feed.task]
   [kafka.connect.event-feed.logging]
   [kafka.connect.event-feed.config :as efc])
  (:import
   [io.logicblocks.kafka.connect.eventfeed
    EventFeedSourceTask]))

(defn start [state-atom props]
  (let [config (efc/configuration props)]
    (log/infof "EventFeedSourceConnector[config: %s] starting..."
      (pr-str config))
    (reset! state-atom
      {:config config
       :properties props})))

(defn stop [state-atom]
  (let [state (deref state-atom)
        config (:config state)]
    (log/infof "EventFeedSourceConnector[config: %s] stopping..."
      (pr-str config))
    (reset! state-atom nil)))

(defn config [_]
  (efc/configuration-definition))

(defn version [_]
  "0.0.1")

(defn task-class [_]
  EventFeedSourceTask)

(defn task-configs [state-atom max-tasks]
  (let [state (deref state-atom)
        props (:properties state)]
    (repeat max-tasks props)))
