(ns kafka.connect.event-feed.events
  (:refer-clojure :exclude [key])
  (:require
   [halboy.navigator :as halnav]
   [halboy.resource :as hal]
   [halboy.json :as haljson]

   [json-path :as jp]

   [kafka.connect.event-feed.config :as efc]
   [kafka.connect.event-feed.records :as efr]))

(defn load-more-events? [resource]
  (not (nil? (hal/get-link resource :next))))

(defn load-page-of-events
  ([navigator link]
   (load-page-of-events navigator link {}))
  ([navigator link parameters]
   (let [navigator (halnav/get navigator link parameters)
         resource (halnav/resource navigator)
         events (hal/get-resource resource :events)]
     [navigator resource events])))

(defn load-new-events [config offset]
  (let [discovery-url (efc/event-feed-discovery-url config)
        events-per-page (efc/event-feed-events-per-page config)]
    (loop [all-events []
           navigator (halnav/discover discovery-url)
           link :events
           parameters
           (cond-> {:pick events-per-page}
             (not (nil? offset)) (assoc :since offset))]
      (let [[navigator resource events] (load-page-of-events
                                          navigator link parameters)
            all-events (into all-events events)]
        (if (load-more-events? resource)
          (recur all-events navigator :next {})
          all-events)))))

(defn event->source-record [config event source-partition]
  (let [key-field-jsonpath (efc/event-key-field-jsonpath config)
        topic-name (efc/topic-name config)
        key (jp/at-path key-field-jsonpath (haljson/resource->map event))
        event-id (hal/get-property event :id)
        record (efr/source-record
                 :source-partition source-partition
                 :source-offset {:offset event-id}
                 :topic-name topic-name
                 :key key
                 :value (haljson/resource->map event))]
    record))

(defn events->source-records [config events source-partition]
  (efr/source-records
    (map #(event->source-record config % source-partition) events)))
