(ns kafka.connect.event-feed.event-feed-test
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
   [kafka.connect.event-feed.test.stubs :as ts]
   [kafka.connect.event-feed.test.data :as td]
   [kafka.connect.event-feed.test.connector :as tcn]))

(def kafka-atom (atom nil))
(def wiremock-atom (atom nil))

(use-fixtures :each
  (ktc/with-kafka kafka-atom)
  (wmf/with-wiremock wiremock-atom))

(deftest fetches-no-events-when-event-feed-empty
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server)
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 5}
         :event-resources [])]
      (tcn/with-connector kafka-connect
        {:name   :event-feed-source
         :config {:connector.class           tcn/connector-class
                  :topic.name                topic-name
                  :eventfeed.discovery.url   (tr/discovery-href wiremock-url)
                  :eventfeed.events.per.page 5}}
        (let [messages
              (tc/consume-if kafka topic-name
                (fn []
                  (let [event-feed-gets
                        (wmc/get-logged-requests
                          :GET (tr/events-path {:pick 5})
                          wiremock-server)]
                    (>= (count event-feed-gets) 10))))]
          (is (= 0 (count messages))))))))

(deftest fetches-single-event-from-one-event-feed-page
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events

        event-resource-id (td/random-event-id)
        event-resource (tr/event-resource wiremock-url
                         {:id   event-resource-id
                          :type :event-type})]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server)
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2}
         :event-resources [event-resource])
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2 :since event-resource-id}
         :event-resources [])]
      (tcn/with-connector kafka-connect
        {:name   :event-feed-source
         :config {:connector.class           tcn/connector-class
                  :topic.name                topic-name
                  :eventfeed.discovery.url   (tr/discovery-href wiremock-url)
                  :eventfeed.events.per.page 2}}
        (let [messages (tc/consume-n kafka topic-name 1)
              message (first messages)
              message-payload (get-in message [:value :payload])]
          (is (= (haljson/resource->map event-resource)
                message-payload)))))))

(deftest fetches-multiple-events-from-one-event-feed-page
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events

        event-resource-1 (tr/event-resource wiremock-url
                           {:id   (td/random-event-id)
                            :type :event-type-1})
        event-resource-2-id (td/random-event-id)
        event-resource-2 (tr/event-resource wiremock-url
                           {:id   event-resource-2-id
                            :type :event-type-2})]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server)
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2}
         :event-resources [event-resource-1 event-resource-2])
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2 :since event-resource-2-id}
         :event-resources [])]
      (tcn/with-connector kafka-connect
        {:name   :event-feed-source
         :config {:connector.class           tcn/connector-class
                  :topic.name                topic-name
                  :eventfeed.discovery.url   (tr/discovery-href wiremock-url)
                  :eventfeed.events.per.page 2}}
        (let [messages (tc/consume-n kafka topic-name 2)
              message-payloads (map #(get-in % [:value :payload])
                                 messages)]
          (is (= [(haljson/resource->map event-resource-1)
                  (haljson/resource->map event-resource-2)]
                message-payloads)))))))

(deftest fetches-multiple-events-from-two-event-feed-pages
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events

        event-resource-1 (tr/event-resource wiremock-url
                           {:id   (td/random-event-id)
                            :type :event-type-1})
        event-resource-2-id (td/random-event-id)
        event-resource-2 (tr/event-resource wiremock-url
                           {:id   event-resource-2-id
                            :type :event-type-2})
        event-resource-3-id (td/random-event-id)
        event-resource-3 (tr/event-resource wiremock-url
                           {:id   event-resource-3-id
                            :type :event-type-3})]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server)
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2}
         :next-link-parameters {:pick 2 :since event-resource-2-id}
         :event-resources [event-resource-1 event-resource-2])
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2 :since event-resource-2-id}
         :event-resources [event-resource-3])
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
              message-payloads (map #(get-in % [:value :payload])
                                 messages)]
          (is (= [(haljson/resource->map event-resource-1)
                  (haljson/resource->map event-resource-2)
                  (haljson/resource->map event-resource-3)]
                message-payloads)))))))

(deftest only-fetches-a-single-page-if-pagination-is-false
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events

        event-resource-1 (tr/event-resource wiremock-url
                           {:id   (td/random-event-id)
                            :type :event-type-1})
        event-resource-2-id (td/random-event-id)
        event-resource-2 (tr/event-resource wiremock-url
                           {:id   event-resource-2-id
                            :type :event-type-2})
        event-resource-3-id (td/random-event-id)
        event-resource-3 (tr/event-resource wiremock-url
                           {:id   event-resource-3-id
                            :type :event-type-3})]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server)
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2}
         :next-link-parameters {:pick 2 :since event-resource-2-id}
         :event-resources [event-resource-1 event-resource-2])
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2 :since event-resource-2-id}
         :event-resources [event-resource-3])
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2 :since event-resource-3-id}
         :event-resources [])]
      (tcn/with-connector kafka-connect
        {:name   :event-feed-source
         :config {:connector.class           tcn/connector-class
                  :topic.name                topic-name
                  :eventfeed.discovery.url   (tr/discovery-href wiremock-url)
                  :eventfeed.events.per.page 2
                  :eventfeed.pagination      false}}
        (let [messages (tc/consume-n kafka topic-name 3)
              message-payloads (map #(get-in % [:value :payload])
                                 messages)]
          (is (= [(haljson/resource->map event-resource-1)
                  (haljson/resource->map event-resource-2)
                  (haljson/resource->map event-resource-3)]
                message-payloads)))))))

(deftest fetches-multiple-events-from-many-event-feed-pages
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events

        event-resource-1 (tr/event-resource wiremock-url
                           {:id   (td/random-event-id)
                            :type :event-type-1})
        event-resource-2-id (td/random-event-id)
        event-resource-2 (tr/event-resource wiremock-url
                           {:id   event-resource-2-id
                            :type :event-type-2})
        event-resource-3 (tr/event-resource wiremock-url
                           {:id   (td/random-event-id)
                            :type :event-type-3})
        event-resource-4-id (td/random-event-id)
        event-resource-4 (tr/event-resource wiremock-url
                           {:id   event-resource-4-id
                            :type :event-type-4})
        event-resource-5-id (td/random-event-id)
        event-resource-5 (tr/event-resource wiremock-url
                           {:id   event-resource-5-id
                            :type :event-type-5})]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server)
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2}
         :next-link-parameters {:pick 2 :since event-resource-2-id}
         :event-resources [event-resource-1 event-resource-2])
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2 :since event-resource-2-id}
         :next-link-parameters {:pick 2 :since event-resource-4-id}
         :event-resources [event-resource-3 event-resource-4])
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2 :since event-resource-4-id}
         :event-resources [event-resource-5])
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2 :since event-resource-5-id}
         :event-resources [])]
      (tcn/with-connector kafka-connect
        {:name   :event-feed-source
         :config {:connector.class           tcn/connector-class
                  :topic.name                topic-name
                  :eventfeed.discovery.url   (tr/discovery-href wiremock-url)
                  :eventfeed.events.per.page 2}}
        (let [messages (tc/consume-n kafka topic-name 5)
              message-payloads (map #(get-in % [:value :payload])
                                 messages)]
          (is (= [(haljson/resource->map event-resource-1)
                  (haljson/resource->map event-resource-2)
                  (haljson/resource->map event-resource-3)
                  (haljson/resource->map event-resource-4)
                  (haljson/resource->map event-resource-5)]
                message-payloads)))))))

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

(deftest uses-provided-jsonpath-to-determine-event-offset-when-specified
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events

        event-resource-1-offset (td/random-uuid)
        event-resource-1 (tr/event-resource wiremock-url
                           {:type    :event-type-1
                            :payload {:offset event-resource-1-offset}})
        event-resource-2-offset (td/random-uuid)
        event-resource-2 (tr/event-resource wiremock-url
                           {:type    :event-type-2
                            :payload {:offset event-resource-2-offset}})
        event-resource-3-offset (td/random-uuid)
        event-resource-3 (tr/event-resource wiremock-url
                           {:type    :event-type-3
                            :payload {:offset event-resource-3-offset}})]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server)
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2}
         :event-resources [event-resource-1])
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2 :since event-resource-1-offset}
         :event-resources [event-resource-2 event-resource-3])
       (ts/events-resource wiremock-server
         :events-link-parameters {:pick 2 :since event-resource-3-offset}
         :event-resources [])]
      (tcn/with-connector kafka-connect
        {:name :event-feed-source
         :config
               {:connector.class               tcn/connector-class
                :topic.name                    topic-name
                :eventfeed.discovery.url       (tr/discovery-href wiremock-url)
                :eventfeed.events.per.page     2
                :events.fields.offset.jsonpath "$.payload.offset"}}
        (let [messages (tc/consume-n kafka topic-name 3)
              message-payloads (map #(get-in % [:value :payload]) messages)]
          (is (= [(haljson/resource->map event-resource-1)
                  (haljson/resource->map event-resource-2)
                  (haljson/resource->map event-resource-3)]
                message-payloads)))))))

(comment
  (compile 'kafka.connect.event-feed.task)
  (compile 'kafka.connect.event-feed.connector))
