(ns kafka.connect.event-feed.events
  (:require
   [halboy.navigator :as halnav]
   [halboy.resource :as hal]
   [halboy.json :as haljson]

   [json-path :as jp]

   [kafka.connect.event-feed.config :as efc]
   [kafka.connect.event-feed.records :as efr]))

(defn has-more-events-available? [resource]
  (not (nil? (hal/get-link resource :next))))

(defn has-not-exceeded-maximum-events-per-poll? [events config]
  (< (count events) (efc/polling-maximum-events-per-poll config)))

(defn load-more-events? [resource events config]
  (and
    (has-not-exceeded-maximum-events-per-poll? events config)
    (has-more-events-available? resource)))

(defn load-page-of-events
  ([navigator link]
   (load-page-of-events navigator link {}))
  ([navigator link parameters]
   (let [navigator (halnav/get navigator link parameters)
         resource (halnav/resource navigator)
         events (hal/get-resource resource :events)]
     [navigator resource events])))

(defn load-new-events [config offset]
  (let [discovery-url
        (efc/event-feed-discovery-url config)
        events-per-page
        (efc/event-feed-events-per-page config)
        per-page-query-parameter-name
        (efc/event-feed-per-page-query-parameter-name config)
        maximum-events
        (efc/polling-maximum-events-per-poll config)]
    (loop [all-events []
           navigator (halnav/discover discovery-url)
           link :events
           parameters
           (cond-> {per-page-query-parameter-name events-per-page}
             (not (nil? offset)) (assoc :since offset))]
      (let [[navigator resource events] (load-page-of-events
                                          navigator link parameters)
            all-events (into all-events events)]
        (if (load-more-events? resource all-events config)
          (recur all-events navigator :next {})
          (take maximum-events all-events))))))

(defn event->key [config event]
  (jp/at-path
    (efc/event-key-field-jsonpath config)
    (haljson/resource->map event)))

(defn event->offset [config event]
  (jp/at-path
    (efc/event-offset-field-jsonpath config)
    (haljson/resource->map event)))

(defn event->source-record [config event source-partition]
  (efr/source-record
    :source-partition source-partition
    :source-offset {:offset (event->offset config event)}
    :topic-name (efc/topic-name config)
    :key (event->key config event)
    :value (haljson/resource->map event)))

(defn events->source-records [config events source-partition]
  (efr/source-records
    (map #(event->source-record config % source-partition) events)))
