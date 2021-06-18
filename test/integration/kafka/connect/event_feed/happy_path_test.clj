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
          (hal/add-resource :events event-resource))]
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
              message (first messages)]
          (clojure.pprint/pprint message)
          (is (= event-id message)))))))

(compile 'kafka.connect.event-feed.task)
(compile 'kafka.connect.event-feed.connector)