(ns kafka.connect.event-feed.feed-test
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

(deftest fetches-no-events-when-event-feed-empty
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server)
       (ts/events-resource wiremock-server
         :events-link {:parameters {:per-page 5}}
         :events {:resources []})]
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
                          :GET (tr/events-path {:parameters {:per-page 5}})
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
         :events-link {:parameters {:per-page 2}}
         :events {:resources [event-resource]})
       (ts/events-resource wiremock-server
         :events-link {:parameters {:per-page 2 :since event-resource-id}}
         :events {:resources []})]
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

(deftest fetches-single-event-from-one-event-feed-page-with-vector
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events
        event-resource-id (td/random-event-id)
        payload  {:vector [{:string-example       "50.0"}]}
        event-resource (tr/event-resource wiremock-url
                         {:id   event-resource-id
                          :type :event-type
                          :payload payload})]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server)
       (ts/events-resource wiremock-server
         :events-link {:parameters {:per-page 2}}
         :events {:resources [event-resource]})
       (ts/events-resource wiremock-server
         :events-link {:parameters {:per-page 2 :since event-resource-id}}
         :events {:resources []})]
      (tcn/with-connector kafka-connect
        {:name   :event-feed-source
         :config {:connector.class           tcn/connector-class
                  :topic.name                topic-name
                  :eventfeed.discovery.url   (tr/discovery-href wiremock-url)
                  :eventfeed.events.per.page 2}}
        (let [messages (tc/consume-n kafka topic-name 1)
              message (first messages)
              message-payload (get-in message [:value :payload])]
          (is (= (haljson/resource->map
                   event-resource)
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
         :events-link {:parameters {:per-page 2}}
         :events {:resources [event-resource-1 event-resource-2]})
       (ts/events-resource wiremock-server
         :events-link {:parameters {:per-page 2 :since event-resource-2-id}}
         :events {:resources []})]
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
         :events-link {:parameters {:per-page 2}}
         :next-link {:parameters {:per-page 2 :since event-resource-2-id}}
         :events {:resources [event-resource-1 event-resource-2]})
       (ts/events-resource wiremock-server
         :events-link {:parameters {:per-page 2 :since event-resource-2-id}}
         :events {:resources [event-resource-3]})
       (ts/events-resource wiremock-server
         :events-link {:parameters {:per-page 2 :since event-resource-3-id}}
         :events {:resources []})]
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
         :events-link {:parameters {:per-page 2}}
         :next-link {:parameters {:per-page 2 :since event-resource-2-id}}
         :events {:resources [event-resource-1 event-resource-2]})
       (ts/events-resource wiremock-server
         :events-link {:parameters {:per-page 2 :since event-resource-2-id}}
         :next-link {:parameters {:per-page 2 :since event-resource-4-id}}
         :events {:resources [event-resource-3 event-resource-4]})
       (ts/events-resource wiremock-server
         :events-link {:parameters {:per-page 2 :since event-resource-4-id}}
         :events {:resources [event-resource-5]})
       (ts/events-resource wiremock-server
         :events-link {:parameters {:per-page 2 :since event-resource-5-id}}
         :events {:resources []})]
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

(deftest uses-provided-jsonpath-to-determine-event-offset-when-specified
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events

        event-resource-1-offset (td/random-uuid-string)
        event-resource-1 (tr/event-resource wiremock-url
                           {:type    :event-type-1
                            :payload {:offset event-resource-1-offset}})
        event-resource-2-offset (td/random-uuid-string)
        event-resource-2 (tr/event-resource wiremock-url
                           {:type    :event-type-2
                            :payload {:offset event-resource-2-offset}})
        event-resource-3-offset (td/random-uuid-string)
        event-resource-3 (tr/event-resource wiremock-url
                           {:type    :event-type-3
                            :payload {:offset event-resource-3-offset}})]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server)
       (ts/events-resource wiremock-server
         :events-link {:parameters {:per-page 2}}
         :events {:resources [event-resource-1]})
       (ts/events-resource wiremock-server
         :events-link {:parameters {:per-page 2 :since event-resource-1-offset}}
         :events {:resources [event-resource-2 event-resource-3]})
       (ts/events-resource wiremock-server
         :events-link {:parameters {:per-page 2 :since event-resource-3-offset}}
         :events {:resources []})]
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

(deftest uses-provided-name-for-per-page-template-parameter
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        discovery-href (tr/discovery-href wiremock-url)

        topic-name :events
        per-page-parameter-name :pick
        events-per-page 2

        event-resource-1-id (td/random-uuid-string)
        event-resource-1 (tr/event-resource wiremock-url
                           {:id   event-resource-1-id
                            :type :event-type-1})
        event-resource-2-id (td/random-uuid-string)
        event-resource-2 (tr/event-resource wiremock-url
                           {:id   event-resource-2-id
                            :type :event-type-2})
        event-resource-3-id (td/random-uuid-string)
        event-resource-3 (tr/event-resource wiremock-url
                           {:id   event-resource-3-id
                            :type :event-type-3})]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server
         {:events-link
          {:parameter-names {:per-page :pick}}})
       (ts/events-resource wiremock-server
         :events-link
         {:parameters      {:pick events-per-page}
          :parameter-names {:per-page :pick}}
         :next-link
         {:parameters      {:pick events-per-page :since event-resource-2-id}
          :parameter-names {:per-page :pick}}
         :events
         {:resources [event-resource-1 event-resource-2]})
       (ts/events-resource wiremock-server
         :events-link
         {:parameters      {:pick events-per-page :since event-resource-2-id}
          :parameter-names {:per-page :pick}}
         :events
         {:resources [event-resource-3]})
       (ts/events-resource wiremock-server
         :events-link
         {:parameters      {:pick events-per-page :since event-resource-3-id}
          :parameter-names {:per-page :pick}}
         :events
         {:resources []})]
      (tcn/with-connector kafka-connect
        {:name :event-feed-source
         :config
         {:connector.class
          tcn/connector-class
          :topic.name
          topic-name
          :eventfeed.discovery.url
          discovery-href
          :eventfeed.events.per.page
          events-per-page
          :eventfeed.template.parameter.name.per.page
          per-page-parameter-name}}
        (let [messages (tc/consume-n kafka topic-name 3)
              message-payloads (map #(get-in % [:value :payload]) messages)]
          (is (= [(haljson/resource->map event-resource-1)
                  (haljson/resource->map event-resource-2)
                  (haljson/resource->map event-resource-3)]
                message-payloads)))))))

(deftest uses-provided-name-for-since-template-parameter
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        discovery-href (tr/discovery-href wiremock-url)

        topic-name :events
        since-parameter-name :after
        events-per-page 2

        event-resource-1-id (td/random-uuid-string)
        event-resource-1 (tr/event-resource wiremock-url
                           {:id   event-resource-1-id
                            :type :event-type-1})
        event-resource-2-id (td/random-uuid-string)
        event-resource-2 (tr/event-resource wiremock-url
                           {:id   event-resource-2-id
                            :type :event-type-2})
        event-resource-3-id (td/random-uuid-string)
        event-resource-3 (tr/event-resource wiremock-url
                           {:id   event-resource-3-id
                            :type :event-type-3})]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server
         {:events-link
          {:parameter-names {:since :after}}})
       (ts/events-resource wiremock-server
         :events-link
         {:parameters {:per-page events-per-page}}
         :next-link
         {:parameters      {:per-page events-per-page
                            :after    event-resource-2-id}
          :parameter-names {:since :after}}
         :events
         {:resources [event-resource-1 event-resource-2]})
       (ts/events-resource wiremock-server
         :events-link
         {:parameters      {:per-page events-per-page
                            :after    event-resource-2-id}
          :parameter-names {:since :after}}
         :events
         {:resources [event-resource-3]})
       (ts/events-resource wiremock-server
         :events-link
         {:parameters      {:per-page events-per-page
                            :after    event-resource-3-id}
          :parameter-names {:since :after}}
         :events
         {:resources []})]
      (tcn/with-connector kafka-connect
        {:name :event-feed-source
         :config
         {:connector.class
          tcn/connector-class
          :topic.name
          topic-name
          :eventfeed.discovery.url
          discovery-href
          :eventfeed.events.per.page
          events-per-page
          :eventfeed.template.parameter.name.since
          since-parameter-name}}
        (let [messages (tc/consume-n kafka topic-name 3)
              message-payloads (map #(get-in % [:value :payload]) messages)]
          (is (= [(haljson/resource->map event-resource-1)
                  (haljson/resource->map event-resource-2)
                  (haljson/resource->map event-resource-3)]
                message-payloads)))))))

(deftest uses-provided-name-for-discovery-events-link
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        discovery-href (tr/discovery-href wiremock-url)

        topic-name :events
        discovery-events-link-name :eventfeed
        events-per-page 2

        event-resource-1-id (td/random-uuid-string)
        event-resource-1 (tr/event-resource wiremock-url
                           {:id   event-resource-1-id
                            :type :event-type-1})
        event-resource-2-id (td/random-uuid-string)
        event-resource-2 (tr/event-resource wiremock-url
                           {:id   event-resource-2-id
                            :type :event-type-2})
        event-resource-3-id (td/random-uuid-string)
        event-resource-3 (tr/event-resource wiremock-url
                           {:id   event-resource-3-id
                            :type :event-type-3})]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server
         {:events-link {:name discovery-events-link-name}})
       (ts/events-resource wiremock-server
         :events-link {:parameters {:per-page events-per-page}}
         :next-link
         {:parameters {:per-page events-per-page :since event-resource-2-id}}
         :events
         {:resources [event-resource-1 event-resource-2]})
       (ts/events-resource wiremock-server
         :events-link
         {:parameters {:per-page events-per-page :since event-resource-2-id}}
         :events
         {:resources [event-resource-3]})
       (ts/events-resource wiremock-server
         :events-link
         {:parameters {:per-page events-per-page :since event-resource-3-id}}
         :events
         {:resources []})]
      (tcn/with-connector kafka-connect
        {:name :event-feed-source
         :config
         {:connector.class
          tcn/connector-class
          :topic.name
          topic-name
          :eventfeed.discovery.url
          discovery-href
          :eventfeed.events.per.page
          events-per-page
          :eventfeed.link.name.discovery.events
          discovery-events-link-name}}
        (let [messages (tc/consume-n kafka topic-name 3)
              message-payloads (map #(get-in % [:value :payload]) messages)]
          (is (= [(haljson/resource->map event-resource-1)
                  (haljson/resource->map event-resource-2)
                  (haljson/resource->map event-resource-3)]
                message-payloads)))))))

(deftest uses-provided-name-for-events-next-link
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        discovery-href (tr/discovery-href wiremock-url)

        topic-name :events
        events-next-link-name :nextpage
        events-per-page 3
        max-events-per-poll 4

        event-resource-1-id (td/random-uuid-string)
        event-resource-1 (tr/event-resource wiremock-url
                           {:id   event-resource-1-id
                            :type :event-type-1})
        event-resource-2-id (td/random-uuid-string)
        event-resource-2 (tr/event-resource wiremock-url
                           {:id   event-resource-2-id
                            :type :event-type-2})
        event-resource-3-id (td/random-uuid-string)
        event-resource-3 (tr/event-resource wiremock-url
                           {:id   event-resource-3-id
                            :type :event-type-3})
        event-resource-4-id (td/random-uuid-string)
        event-resource-4 (tr/event-resource wiremock-url
                           {:id   event-resource-4-id
                            :type :event-type-4})
        event-resource-5-id (td/random-uuid-string)
        event-resource-5 (tr/event-resource wiremock-url
                           {:id   event-resource-5-id
                            :type :event-type-5})
        event-resource-6-id (td/random-uuid-string)
        event-resource-6 (tr/event-resource wiremock-url
                           {:id   event-resource-6-id
                            :type :event-type-6})
        event-resource-7-id (td/random-uuid-string)
        event-resource-7 (tr/event-resource wiremock-url
                           {:id   event-resource-7-id
                            :type :event-type-7})]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server)
       (ts/events-resource wiremock-server
         :events-link {:parameters {:per-page events-per-page}}
         :next-link
         {:name       events-next-link-name
          :parameters {:per-page events-per-page :since event-resource-3-id}}
         :events
         {:resources [event-resource-1 event-resource-2 event-resource-3]})
       (ts/events-resource wiremock-server
         :events-link
         {:parameters {:per-page events-per-page :since event-resource-3-id}}
         :events
         {:resources [event-resource-4 event-resource-5 event-resource-6]})
       (ts/events-resource wiremock-server
         :events-link
         {:parameters {:per-page events-per-page :since event-resource-4-id}}
         :events
         {:resources [event-resource-5 event-resource-6 event-resource-7]})
       (ts/events-resource wiremock-server
         :events-link
         {:parameters {:per-page events-per-page :since event-resource-7-id}}
         :events
         {:resources []})]
      (tcn/with-connector kafka-connect
        {:name :event-feed-source
         :config
         {:connector.class
          tcn/connector-class
          :topic.name
          topic-name
          :polling.max.events.per.poll
          max-events-per-poll
          :eventfeed.discovery.url
          discovery-href
          :eventfeed.events.per.page
          events-per-page
          :eventfeed.link.name.events.next
          events-next-link-name}}
        (let [messages (tc/consume-n kafka topic-name 7)
              message-payloads (map #(get-in % [:value :payload]) messages)]
          (is (= [(haljson/resource->map event-resource-1)
                  (haljson/resource->map event-resource-2)
                  (haljson/resource->map event-resource-3)
                  (haljson/resource->map event-resource-4)
                  (haljson/resource->map event-resource-5)
                  (haljson/resource->map event-resource-6)
                  (haljson/resource->map event-resource-7)]
                message-payloads)))))))

(deftest uses-provided-name-for-events-embedded-resources
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events
        events-per-page 2
        events-embedded-resource-name :items

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
         :events-link {:parameters {:per-page events-per-page}}
         :next-link
         {:parameters {:per-page events-per-page :since event-resource-2-id}}
         :events
         {:resource-name events-embedded-resource-name
          :resources     [event-resource-1 event-resource-2]})
       (ts/events-resource wiremock-server
         :events-link
         {:parameters {:per-page events-per-page :since event-resource-2-id}}
         :events
         {:resource-name events-embedded-resource-name
          :resources     [event-resource-3]})
       (ts/events-resource wiremock-server
         :events-link
         {:parameters {:per-page events-per-page :since event-resource-3-id}}
         :events
         {:resource-name events-embedded-resource-name
          :resources     []})]
      (tcn/with-connector kafka-connect
        {:name
         :event-feed-source
         :config
         {:connector.class
          tcn/connector-class
          :topic.name
          topic-name
          :eventfeed.discovery.url
          (tr/discovery-href wiremock-url)
          :eventfeed.events.per.page
          events-per-page
          :eventfeed.embedded.resource.name.events
          events-embedded-resource-name}}
        (let [messages (tc/consume-n kafka topic-name 3)
              message-payloads (map #(get-in % [:value :payload])
                                 messages)]
          (is (= [(haljson/resource->map event-resource-1)
                  (haljson/resource->map event-resource-2)
                  (haljson/resource->map event-resource-3)]
                message-payloads)))))))

(deftest caches-discovery
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events]
    (wmc/with-stubs
      [(ts/discovery-resource wiremock-server)
       (ts/events-resource wiremock-server
         :events-link {:parameters {:per-page 5}}
         :events {:resources []})]
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
                          :GET (tr/events-path {:parameters {:per-page 5}})
                          wiremock-server)]
                    (>= (count event-feed-gets) 10))))]
          (is (= 1 (count (wmc/get-logged-requests :GET "/" wiremock-server))))
          (is (= 0 (count messages))))))))

(deftest does-not-cache-failed-discovery
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events]
    (wmc/with-stubs
      [(ts/discovery-internal-server-error wiremock-server {:new "1"})
       (ts/discovery-resource wiremock-server {} {:required "1"})
       (ts/events-resource wiremock-server
         :events-link {:parameters {:per-page 5}}
         :events {:resources []})]
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
                          :GET (tr/events-path {:parameters {:per-page 5}})
                          wiremock-server)]
                    (>= (count event-feed-gets) 10))))]
          (is (= 2 (count (wmc/get-logged-requests :GET "/" wiremock-server))))
          (is (= 0 (count messages))))))))
