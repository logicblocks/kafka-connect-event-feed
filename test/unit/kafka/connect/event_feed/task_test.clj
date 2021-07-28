(ns kafka.connect.event-feed.task-test
  (:require
   [clojure.test :refer :all]

   [org.httpkit.fake :as httpfake]

   [kafka.connect.event-feed.test.resources :as tr]
   [kafka.connect.event-feed.test.data :as td]
   [kafka.connect.event-feed.test.stubs.httpkit :as ts]
   [kafka.connect.event-feed.utils :as efu])
  (:import
   [io.logicblocks.kafka.connect.eventfeed EventFeedSourceTask]))

(deftest fetches-all-pages-if-pagination-is-true
  (let [base-url "http://eventfeed.example.com"

        topic-name :events

        source-task (EventFeedSourceTask.)
        _ (.start
            source-task
            (efu/clojure-data->java-data
              {:topic.name                topic-name
               :eventfeed.discovery.url   (tr/discovery-href base-url)
               :eventfeed.events.per.page 2
               :eventfeed.pagination      true}))

        event-resource-1-id (td/random-event-id)
        event-resource-1 (tr/event-resource base-url
                           {:id   event-resource-1-id
                            :type :event-type-1})
        event-resource-2-id (td/random-event-id)
        event-resource-2 (tr/event-resource base-url
                           {:id   event-resource-2-id
                            :type :event-type-2})
        event-resource-3-id (td/random-event-id)
        event-resource-3 (tr/event-resource base-url
                           {:id   event-resource-3-id
                            :type :event-type-3})]
    (httpfake/with-fake-http
      (concat
        (ts/discovery-resource base-url)
        (ts/events-resource base-url
          :events-link-parameters {:pick 2}
          :next-link-parameters {:pick 2 :since event-resource-2-id}
          :event-resources [event-resource-1 event-resource-2])
        (ts/events-resource base-url
          :events-link-parameters {:pick 2 :since event-resource-2-id}
          :event-resources [event-resource-3])
        (ts/events-resource base-url
          :events-link-parameters {:pick 2 :since event-resource-3-id}
          :event-resources []))

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
  (let [base-url "http://eventfeed.example.com"

        topic-name :events

        source-task (EventFeedSourceTask.)
        _ (.start
            source-task
            (efu/clojure-data->java-data
              {:topic.name                topic-name
               :eventfeed.discovery.url   (tr/discovery-href base-url)
               :eventfeed.events.per.page 2
               :eventfeed.pagination      false}))

        event-resource-1-id (td/random-event-id)
        event-resource-1 (tr/event-resource base-url
                           {:id   event-resource-1-id
                            :type :event-type-1})
        event-resource-2-id (td/random-event-id)
        event-resource-2 (tr/event-resource base-url
                           {:id   event-resource-2-id
                            :type :event-type-2})
        event-resource-3-id (td/random-event-id)
        event-resource-3 (tr/event-resource base-url
                           {:id   event-resource-3-id
                            :type :event-type-3})]
    (httpfake/with-fake-http
      (concat
        (ts/discovery-resource base-url)
        (ts/events-resource base-url
          :events-link-parameters {:pick 2}
          :next-link-parameters {:pick 2 :since event-resource-2-id}
          :event-resources [event-resource-1 event-resource-2])
        (ts/events-resource base-url
          :events-link-parameters {:pick 2 :since event-resource-2-id}
          :event-resources [event-resource-3])
        (ts/events-resource base-url
          :events-link-parameters {:pick 2 :since event-resource-3-id}
          :event-resources []))

      (let [messages (.poll source-task)
            message-payloads (map #(->
                                     (.sourceOffset %)
                                     (.values)
                                     (first))
                               messages)]
        (is (= [event-resource-1-id
                event-resource-2-id]
              message-payloads))))))
