(ns kafka.connect.event-feed.task-test
  (:require
   [clojure.test :refer :all]

   [clj-wiremock.core :as wmc]
   [clj-wiremock.utils :as wmu]
   [clj-wiremock.fixtures :as wmf]

   [kafka.connect.event-feed.test.resources :as tr]
   [kafka.connect.event-feed.test.data :as td]
   [kafka.connect.event-feed.test.stubs :as ts]
   [kafka.connect.event-feed.utils :as efu])
  (:import
   [io.logicblocks.kafka.connect.eventfeed EventFeedSourceTask]))

(def wiremock-atom (atom nil))

(use-fixtures :each
  (wmf/with-wiremock wiremock-atom))

(deftest fetches-all-pages-if-pagination-is-true
  (let [wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events

        source-task (EventFeedSourceTask.)
        _ (.start
            source-task
            (efu/clojure-data->java-data
              {:topic.name                topic-name
               :eventfeed.discovery.url   (tr/discovery-href wiremock-url)
               :eventfeed.events.per.page 2
               :eventfeed.pagination      true}))

        event-resource-1-id (td/random-event-id)
        event-resource-1 (tr/event-resource wiremock-url
                           {:id   event-resource-1-id
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
      (let [messages (.poll source-task)
            message-payloads (map #(->
                                     (.sourceOffset %)
                                     (.values)
                                     (first))
                               messages)]
        (is (= [event-resource-1-id
                event-resource-2-id
                event-resource-3-id]
              message-payloads))))))

(deftest only-fetches-a-single-page-if-pagination-is-false
  (let [wiremock-server @wiremock-atom
        wiremock-url (wmu/base-url wiremock-server)

        topic-name :events

        source-task (EventFeedSourceTask.)
        _ (.start
            source-task
            (efu/clojure-data->java-data
              {:topic.name                topic-name
               :eventfeed.discovery.url   (tr/discovery-href wiremock-url)
               :eventfeed.events.per.page 2
               :eventfeed.pagination      false}))

        event-resource-1-id (td/random-event-id)
        event-resource-1 (tr/event-resource wiremock-url
                           {:id   event-resource-1-id
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
      (let [messages (.poll source-task)
            message-payloads (map #(->
                                     (.sourceOffset %)
                                     (.values)
                                     (first))
                               messages)]
        (is (= [event-resource-1-id
                event-resource-2-id]
              message-payloads))))))
