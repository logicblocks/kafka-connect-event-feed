(ns kafka.connect.event-feed.happy-path-test
  (:require
   [clojure.test :refer :all]

   [halboy.json :as haljson]

   [clj-wiremock.fixtures :as wmf]
   [clj-wiremock.core :as wmc]
   [clj-wiremock.utils :as wmu]

   [kafka.testing.combined :as ktc]
   [kafka.testing.connect :as ktkc]

   [kafka.connect.client.core :as kcc]

   [kafka.connect.event-feed.utils :as efu]

   [kafka.connect.event-feed.test.logging]
   [kafka.connect.event-feed.test.resources :as tr]
   [kafka.connect.event-feed.test.consumer :as tc]
   [kafka.connect.event-feed.test.stubs :as ts]
   [kafka.connect.event-feed.test.data :as td]))

(def connector-class
  "io.logicblocks.kafka.connect.eventfeed.EventFeedSourceConnector")

(def kafka-atom (atom nil))
(def wiremock-atom (atom nil))

(use-fixtures :each
  (ktc/with-kafka kafka-atom)
  (wmf/with-wiremock wiremock-atom))

(defmacro with-connector [kafka-connect options & body]
  `(let [admin-url# (ktkc/admin-url ~kafka-connect)
         client# (kcc/client {:url admin-url#})]
     (kcc/add-connector client# (:name ~options)
       (efu/clojure-data->java-data (:config ~options)))
     ~@body))

(deftest fetches-no-events-when-event-feed-empty
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server)
       (ts/events-resource wiremock-server
         :event-resources [])]
      (with-connector kafka-connect
        {:name   :event-feed-source
         :config {:connector.class         connector-class
                  :topic.name              topic-name
                  :eventfeed.discovery.url (tr/discovery-href wiremock-url)}}
        (let [messages
              (tc/consume-if kafka topic-name
                (fn []
                  (let [event-feed-gets
                        (wmc/get-logged-requests
                          :GET (tr/events-path) wiremock-server)]
                    (>= (count event-feed-gets) 10))))]
          (is (= 0 (count messages))))))))

(deftest fetches-single-event-from-event-feed
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events

        event-resource (tr/event-resource wiremock-url
                         {:event-id (td/random-event-id)})]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server)
       (ts/events-resource wiremock-server
         :event-resources [event-resource])]
      (with-connector kafka-connect
        {:name   :event-feed-source
         :config {:connector.class         connector-class
                  :topic.name              topic-name
                  :eventfeed.discovery.url (tr/discovery-href wiremock-url)}}
        (let [messages (tc/consume-n kafka topic-name 1)
              message (first messages)
              message-payload (get-in message [:value :payload])]
          (is (= (haljson/resource->map event-resource)
                message-payload)))))))

(deftest fetches-multiple-events-from-event-feed
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events

        event-resource-1 (tr/event-resource wiremock-url
                           {:event-id (td/random-event-id)})
        event-resource-2 (tr/event-resource wiremock-url
                           {:event-id (td/random-event-id)})]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server)
       (ts/events-resource wiremock-server
         :event-resources [event-resource-1 event-resource-2])]
      (with-connector kafka-connect
        {:name   :event-feed-source
         :config {:connector.class         connector-class
                  :topic.name              topic-name
                  :eventfeed.discovery.url (tr/discovery-href wiremock-url)}}
        (let [messages (tc/consume-n kafka topic-name 2)
              message-payloads (map #(get-in % [:value :payload])
                                 messages)]
          (is (= [(haljson/resource->map event-resource-1)
                  (haljson/resource->map event-resource-2)]
                message-payloads)))))))

; full page
; many pages

(comment
  (compile 'kafka.connect.event-feed.task)
  (compile 'kafka.connect.event-feed.connector))
