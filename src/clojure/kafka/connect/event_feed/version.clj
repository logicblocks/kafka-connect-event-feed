(ns kafka.connect.event-feed.version)

(defmacro version []
  `~(System/getProperty "kafka.connect.event-feed.version"))
