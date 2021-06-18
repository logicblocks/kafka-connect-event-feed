(ns kafka.connect.event-feed.test.logging
  (:import
   [org.slf4j.bridge SLF4JBridgeHandler]))

(SLF4JBridgeHandler/removeHandlersForRootLogger)
(SLF4JBridgeHandler/install)
