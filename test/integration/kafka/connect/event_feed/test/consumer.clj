(ns kafka.connect.event-feed.test.consumer
  (:require
   [jackdaw.client :as jc]
   [jackdaw.serdes.json :as json-serdes]

   [kafka.testing.broker :as ktb]
   [kafka.connect.event-feed.utils :as efu]
   [kafka.connect.event-feed.test.data :as data])
  (:import [java.time Duration]))

(defn consumer
  [kafka topic-name &
   {:keys [key-serde value-serde]
    :or   {key-serde   (json-serdes/serde)
           value-serde (json-serdes/serde)}}]
  (jc/subscribed-consumer
    (efu/clojure-data->java-data
      {:auto.offset.reset        :earliest
       :allow.auto.create.topics false
       :bootstrap.servers        (ktb/bootstrap-servers kafka)
       :group.id                 (data/random-uuid)})
    [{:topic-name  (name topic-name)
      :key-serde   (or key-serde (json-serdes/serde))
      :value-serde (or value-serde (json-serdes/serde))}]))

(defn poll [consumer & {:keys [timeout-ms]}]
  (jc/poll consumer
    (Duration/ofMillis timeout-ms)))

(defn consume-if
  [kafka topic-name condition
   & {:keys [interval-ms
             max-attempts
             poll-timeout-ms
             key-serde
             value-serde]
      :or   {interval-ms     100
             max-attempts    100
             poll-timeout-ms 250}}]
  (with-open [consumer (consumer kafka topic-name
                         :key-serde key-serde
                         :value-serde value-serde)]
    (loop [attempt 0]
      (if (= attempt max-attempts)
        (throw (IllegalStateException.
                 (str
                   "Failed to meet condition"
                   " to consume from topic " topic-name
                   " within " (* (+ interval-ms poll-timeout-ms) max-attempts)
                   " ms.")))
        (if (condition)
          (poll consumer
            :timeout-ms poll-timeout-ms)
          (do
            (Thread/sleep interval-ms)
            (recur (+ attempt 1))))))))

(defn consume-n
  [kafka topic-name n
   & {:keys [interval-ms
             max-attempts
             poll-timeout-ms
             key-serde
             value-serde]
      :or   {interval-ms     100
             max-attempts    100
             poll-timeout-ms 250}}]
  (with-open [consumer (consumer kafka topic-name
                         :key-serde key-serde
                         :value-serde value-serde)]
    (loop [attempt 0
           messages []]
      (if (= attempt max-attempts)
        (throw (IllegalStateException.
                 (str
                   "Failed to consume " n
                   " messages from topic " topic-name
                   " within " (* (+ interval-ms poll-timeout-ms) max-attempts)
                   " ms.")))
        (let [all-messages
              (into messages (poll consumer :timeout-ms poll-timeout-ms))]
          (if (>= (count all-messages) n)
            (take n all-messages)
            (do
              (Thread/sleep interval-ms)
              (recur (+ attempt 1) all-messages))))))))
