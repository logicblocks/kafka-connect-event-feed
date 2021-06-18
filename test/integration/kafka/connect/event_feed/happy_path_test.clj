(ns kafka.connect.event-feed.happy-path-test
  (:require
   [clojure.test :refer :all]

   [halboy.resource :as hal]
   [halboy.json :as haljson]

   [clj-wiremock.fixtures :as wmf]
   [clj-wiremock.core :as wmc]
   [clj-wiremock.server :as wms]

   [kafka.testing.combined :as ktc]
   [kafka.testing.connect :as ktkc]

   [kafka.connect.client.core :as kcc]

   [kafka.connect.event-feed.test.logging]
   [kafka.connect.event-feed.test.consumer :as test-consumer]
   [kafka.connect.event-feed.test.data :as test-data]))

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
     (kcc/add-connector client# (:name ~options) (:config ~options))
     ~@body))

(deftest fetches-no-events-when-event-feed-empty
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom

        topic-name "events"

        discovery-href (wms/url wiremock-server "/")
        discovery-resource
        (-> (hal/new-resource discovery-href)
          (hal/add-link :events
            {:href      (wms/url wiremock-server "/events{?since,pick}")
             :templated true}))

        events-resource
        (-> (hal/new-resource (wms/url wiremock-server "/events"))
          (hal/add-href :discovery discovery-href)
          (hal/add-link :events [])
          (hal/add-resource :events []))]
    (with-connector kafka-connect
      {:name   "event-feed-source"
       :config {"connector.class"         connector-class
                "topic.name"              topic-name
                "eventfeed.discovery.url" discovery-href}}
      (wmc/with-stubs
        [{:server wiremock-server
          :req    [:GET "/"]
          :res    [200 {:body (haljson/resource->json discovery-resource)}]}
         {:server wiremock-server
          :req    [:GET "/events"]
          :res    [200 {:body (haljson/resource->json events-resource)}]}]
        (let [messages
              (test-consumer/consume-if kafka topic-name
                (fn []
                  (let [event-feed-gets
                        (wmc/get-logged-requests
                          :GET "/events" wiremock-server)]
                    (>= (count event-feed-gets) 10))))]
          (is (= 0 (count messages))))))))

(deftest fetches-single-event-from-event-feed
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom

        topic-name "events"

        discovery-href (wms/url wiremock-server "/")
        discovery-resource
        (-> (hal/new-resource discovery-href)
          (hal/add-link :events
            {:href      (wms/url wiremock-server "/events{?since,pick}")
             :templated true}))

        event-id (test-data/random-event-id)
        event-href (wms/url wiremock-server (str "/events/" event-id))
        event-resource (hal/new-resource event-href)

        events-resource
        (-> (hal/new-resource (wms/url wiremock-server "/events"))
          (hal/add-href :discovery discovery-href)
          (hal/add-href :events event-href)
          (hal/add-resource :events [event-resource]))]
    (with-connector kafka-connect
      {:name   "event-feed-source"
       :config {"connector.class"         connector-class
                "topic.name"              topic-name
                "eventfeed.discovery.url" discovery-href}}
      (wmc/with-stubs
        [{:server wiremock-server
          :req    [:GET "/"]
          :res    [200 {:body (haljson/resource->json discovery-resource)}]}
         {:server wiremock-server
          :req    [:GET "/events"]
          :res    [200 {:body (haljson/resource->json events-resource)}]}]
        (let [messages (test-consumer/consume-n kafka topic-name 1)
              message (first messages)
              message-payload (get-in message [:value :payload])]
          (is (= (haljson/resource->map event-resource)
                message-payload)))))))

(deftest fetches-multiple-events-from-event-feed
  (let [kafka (ktc/kafka @kafka-atom)
        kafka-connect (ktc/kafka-connect @kafka-atom)
        wiremock-server @wiremock-atom

        topic-name "events"

        discovery-href (wms/url wiremock-server "/")
        discovery-resource
        (-> (hal/new-resource discovery-href)
          (hal/add-link :events
            {:href      (wms/url wiremock-server "/events{?since,pick}")
             :templated true}))

        event-id-1 (test-data/random-event-id)
        event-href-1 (wms/url wiremock-server (str "/events/" event-id-1))
        event-resource-1 (hal/new-resource event-href-1)

        event-id-2 (test-data/random-event-id)
        event-href-2 (wms/url wiremock-server (str "/events/" event-id-2))
        event-resource-2 (hal/new-resource event-href-2)

        events-resource
        (-> (hal/new-resource (wms/url wiremock-server "/events"))
          (hal/add-href :discovery discovery-href)
          (hal/add-href :events event-href-1)
          (hal/add-href :events event-href-2)
          (hal/add-resource :events event-resource-1)
          (hal/add-resource :events event-resource-2))]
    (with-connector kafka-connect
      {:name   "event-feed-source"
       :config {"connector.class"         connector-class
                "topic.name"              topic-name
                "eventfeed.discovery.url" discovery-href}}
      (wmc/with-stubs
        [{:server wiremock-server
          :req    [:GET "/"]
          :res    [200 {:body (haljson/resource->json discovery-resource)}]}
         {:server wiremock-server
          :req    [:GET "/events"]
          :res    [200 {:body (haljson/resource->json events-resource)}]}]
        (let [messages (test-consumer/consume-n kafka topic-name 2)
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
