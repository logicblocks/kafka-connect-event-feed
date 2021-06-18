(ns kafka.connect.event-feed.logging
  (:require
   [clojure.tools.logging :as ctl]
   [clojure.tools.logging.impl :as ctli]))

(alter-var-root #'ctl/*logger-factory* (fn [_] (ctli/slf4j-factory)))
