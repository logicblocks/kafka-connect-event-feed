(ns kafka.connect.event-feed.test.stubs.wiremock
  (:require
   [clj-wiremock.utils :as wmu]

   [halboy.json :as haljson]

   [kafka.connect.event-feed.test.resources :as tr]))

(defn discovery-resource
  ([wiremock-server]
   (discovery-resource wiremock-server {}))
  ([wiremock-server options]
   (let [wiremock-url (wmu/base-url wiremock-server)]
     {:server wiremock-server
      :req    [:GET (tr/discovery-path)]
      :res    [200 {:body
                    (haljson/resource->json
                      (tr/discovery-resource wiremock-url options))}]})))

(defn events-resource
  [wiremock-server & {:keys [events-link
                             next-link
                             events]}]
  (let [wiremock-url (wmu/base-url wiremock-server)]
    {:server wiremock-server
     :req    [:GET (tr/events-path events-link)]
     :res    [200 {:body
                   (haljson/resource->json
                     (tr/events-resource wiremock-url
                       :events-link events-link
                       :next-link next-link
                       :events events))}]}))
