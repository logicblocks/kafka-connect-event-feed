(ns kafka.connect.event-feed.test.stubs.wiremock
  (:require
   [clj-wiremock.utils :as wmu]

   [halboy.json :as haljson]

   [kafka.connect.event-feed.test.resources :as tr]))

(defn discovery-resource
  ([wiremock-server]
   (discovery-resource wiremock-server {}))
  ([wiremock-server options]
   (discovery-resource wiremock-server options {}))
  ([wiremock-server options state]
   (let [wiremock-url (wmu/base-url wiremock-server)]
     {:server wiremock-server
      :req    [:GET (tr/discovery-path)]
      :res    [200 {:body
                    (haljson/resource->json
                      (tr/discovery-resource wiremock-url options))}]
      :state state})))

(defn discovery-internal-server-error
  [wiremock-server state]
  {:server wiremock-server
   :req    [:GET (tr/discovery-path)]
   :res    [500]
   :state  state})

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
