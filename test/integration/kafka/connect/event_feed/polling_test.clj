(ns kafka.connect.event-feed.polling-test
  (:require
   [clojure.test :refer :all]

   [halboy.json :as haljson]

   [clj-wiremock.fixtures :as wmf]
   [clj-wiremock.core :as wmc]
   [clj-wiremock.utils :as wmu]

   [kafka.testing.combined :as ktc]

   [kafka.connect.event-feed.test.logging]
   [kafka.connect.event-feed.test.resources :as tr]
   [kafka.connect.event-feed.test.consumer :as tc]
   [kafka.connect.event-feed.test.stubs.wiremock :as ts]
   [kafka.connect.event-feed.test.data :as td]
   [kafka.connect.event-feed.test.connector :as tcn]))

(def kafka-atom (atom nil))
(def wiremock-atom (atom nil))

(use-fixtures :each
  (ktc/with-kafka kafka-atom)
  (wmf/with-wiremock wiremock-atom))

(deftest fetches-new-events-once-available
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events

        event-resource-1-id (td/random-event-id)
        event-resource-1 (tr/event-resource wiremock-url
                           {:id   event-resource-1-id
                            :type :event-type-1})
        event-resource-2 (tr/event-resource wiremock-url
                           {:id   (td/random-event-id)
                            :type :event-type-2})
        event-resource-3-id (td/random-event-id)
        event-resource-3 (tr/event-resource wiremock-url
                           {:id   event-resource-3-id
                            :type :event-type-3})]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server)
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2}
         :event-resources [event-resource-1])
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2 :since event-resource-1-id}
         :event-resources [event-resource-2 event-resource-3])
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2 :since event-resource-3-id}
         :event-resources [])]
      (tcn/with-connector kafka-connect
        {:name   :event-feed-source
         :config {:connector.class           tcn/connector-class
                  :topic.name                topic-name
                  :eventfeed.discovery.url   (tr/discovery-href wiremock-url)
                  :eventfeed.events.per.page 2}}
        (let [messages (tc/consume-n kafka topic-name 3)
              message-payloads (map #(get-in % [:value :payload]) messages)]
          (is (= [(haljson/resource->map event-resource-1)
                  (haljson/resource->map event-resource-2)
                  (haljson/resource->map event-resource-3)]
                message-payloads)))))))

(deftest waits-200-milliseconds-between-fetches-by-default
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events

        event-resource-1-id (td/random-event-id)
        event-resource-1 (tr/event-resource wiremock-url
                           {:id   event-resource-1-id
                            :type :event-type-1})
        event-resource-2 (tr/event-resource wiremock-url
                           {:id   (td/random-event-id)
                            :type :event-type-2})
        event-resource-3-id (td/random-event-id)
        event-resource-3 (tr/event-resource wiremock-url
                           {:id   event-resource-3-id
                            :type :event-type-3})]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server)
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2}
         :event-resources [event-resource-1])
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2 :since event-resource-1-id}
         :event-resources [event-resource-2 event-resource-3])
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2 :since event-resource-3-id}
         :event-resources [])]
      (tcn/with-connector kafka-connect
        {:name   :event-feed-source
         :config {:connector.class           tcn/connector-class
                  :topic.name                topic-name
                  :eventfeed.discovery.url   (tr/discovery-href wiremock-url)
                  :eventfeed.events.per.page 2}}
        (let [_ (tc/consume-n kafka topic-name 3)
              all-requests (wmc/request-journal wiremock-server)
              event-requests
              (filter
                #(re-matches #".*events.*" (get-in % [:request :absoluteUrl]))
                all-requests)
              event-request-timestamps
              (map
                #(get-in % [:request :loggedDate])
                event-requests)
              event-request-intervals
              (:intervals
               (reduce
                 (fn [{:keys [intervals last]} timestamp]
                   (let [interval (- timestamp last)]
                     {:intervals (conj intervals interval)
                      :last      timestamp}))
                 {:intervals [] :last (first event-request-timestamps)}
                 (rest event-request-timestamps)))
              event-request-interval-average
              (/ (apply + event-request-intervals)
                (count event-request-intervals))]
          (is (> 300 event-request-interval-average 200)))))))

(deftest waits-the-specified-interval-between-fetches-when-provided
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events

        event-resource-1-id (td/random-event-id)
        event-resource-1 (tr/event-resource wiremock-url
                           {:id   event-resource-1-id
                            :type :event-type-1})
        event-resource-2 (tr/event-resource wiremock-url
                           {:id   (td/random-event-id)
                            :type :event-type-2})
        event-resource-3-id (td/random-event-id)
        event-resource-3 (tr/event-resource wiremock-url
                           {:id   event-resource-3-id
                            :type :event-type-3})]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server)
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2}
         :event-resources [event-resource-1])
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2 :since event-resource-1-id}
         :event-resources [event-resource-2 event-resource-3])
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2 :since event-resource-3-id}
         :event-resources [])]
      (tcn/with-connector kafka-connect
        {:name   :event-feed-source
         :config {:connector.class             tcn/connector-class
                  :topic.name                  topic-name
                  :eventfeed.fetch.interval.ms 100
                  :eventfeed.discovery.url     (tr/discovery-href wiremock-url)
                  :eventfeed.events.per.page   2}}
        (let [_ (tc/consume-n kafka topic-name 3)
              all-requests (wmc/request-journal wiremock-server)
              event-requests
              (filter
                #(re-matches #".*events.*" (get-in % [:request :absoluteUrl]))
                all-requests)
              event-request-timestamps
              (map
                #(get-in % [:request :loggedDate])
                event-requests)
              event-request-intervals
              (:intervals
               (reduce
                 (fn [{:keys [intervals last]} timestamp]
                   (let [interval (- timestamp last)]
                     {:intervals (conj intervals interval)
                      :last      timestamp}))
                 {:intervals [] :last (first event-request-timestamps)}
                 (rest event-request-timestamps)))
              event-request-interval-average
              (/ (apply + event-request-intervals)
                (count event-request-intervals))]
          (is (> 200 event-request-interval-average 100)))))))
