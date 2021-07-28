(ns kafka.connect.event-feed.test.stubs.wiremock
  (:require
   [clj-wiremock.utils :as wmu]

   [halboy.json :as haljson]

   [kafka.connect.event-feed.test.resources :as tr]))

(defn discovery-resource [wiremock-server]
  (let [wiremock-url (wmu/base-url wiremock-server)]
    {:server wiremock-server
     :req    [:GET (tr/discovery-path)]
     :res    [200 {:body
                   (haljson/resource->json
                     (tr/discovery-resource wiremock-url))}]}))

(defn events-resource
  [wiremock-server & {:keys [events-link-parameters
                             next-link-parameters
                             event-resources]}]
  (let [wiremock-url (wmu/base-url wiremock-server)]
    {:server wiremock-server
     :req    [:GET (tr/events-path events-link-parameters)]
     :res    [200 {:body
                   (haljson/resource->json
                     (tr/events-resource wiremock-url
                       :events-link-parameters events-link-parameters
                       :next-link-parameters next-link-parameters
                       :event-resources event-resources))}]}))
