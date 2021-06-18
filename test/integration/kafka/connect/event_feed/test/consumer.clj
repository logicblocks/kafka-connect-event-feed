(ns kafka.connect.event-feed.test.consumer
  (:require
   [jackdaw.client :as jc]
   [jackdaw.serdes.json :as json-serdes]

   [kafka.testing.broker :as ktb]
   [kafka.connect.event-feed.test.data :as data])
  (:import [java.time Duration]))

(defn consume-if
  [kafka topic-name condition
   & {:keys [interval-ms
             max-attempts
             poll-timeout-ms
             key-serde
             value-serde]
      :or   {interval-ms     100
             max-attempts    50
             poll-timeout-ms 5000
             key-serde       (json-serdes/serde)
             value-serde     (json-serdes/serde)}}]
  (let [group-id (data/random-uuid)
        bootstrap-servers (ktb/bootstrap-servers kafka)]
    (with-open [consumer (jc/subscribed-consumer
                           {"bootstrap.servers" bootstrap-servers
                            "group.id"          group-id}
                           [{:topic-name  topic-name
                             :key-serde   key-serde
                             :value-serde value-serde}])]
      (loop [attempt 0]
        (if (= attempt max-attempts)
          (throw (IllegalStateException.
                   (str
                     "Failed to meet condition"
                     " to consume from topic " topic-name
                     " within " (* interval-ms max-attempts) " ms.")))
          (if (condition)
            (jc/poll consumer
              (Duration/ofMillis poll-timeout-ms))
            (do
              (Thread/sleep interval-ms)
              (recur (+ attempt 1)))))))))

(defn consume-n
  [kafka topic-name n
   & {:keys [interval-ms
             max-attempts
             poll-timeout-ms
             key-serde
             value-serde]
      :or   {interval-ms     100
             max-attempts    50
             poll-timeout-ms 5000
             key-serde       (json-serdes/serde)
             value-serde     (json-serdes/serde)}}]
  (let [group-id (data/random-uuid)
        bootstrap-servers (ktb/bootstrap-servers kafka)]
    (with-open [consumer (jc/subscribed-consumer
                           {"bootstrap.servers" bootstrap-servers
                            "group.id"          group-id}
                           [{:topic-name  topic-name
                             :key-serde   key-serde
                             :value-serde value-serde}])]
      (loop [attempt 0
             messages []]
        (if (= attempt max-attempts)
          (throw (IllegalStateException.
                   (str
                     "Failed to consume " n
                     " messages from topic " topic-name
                     " within " (* interval-ms max-attempts) " ms.")))
          (let [polled-messages (jc/poll consumer
                                  (Duration/ofMillis poll-timeout-ms))
                all-messages (concat messages polled-messages)]
            (if (>= (count all-messages) n)
              all-messages
              (do
                (Thread/sleep interval-ms)
                (recur (+ attempt 1) all-messages)))))))))
