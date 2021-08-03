(ns kafka.connect.event-feed.events
  (:require
   [halboy.navigator :as halnav]
   [halboy.resource :as hal]
   [halboy.json :as haljson]

   [json-path :as jp]

   [kafka.connect.event-feed.config :as efc]
   [kafka.connect.event-feed.records :as efr]))

(defn has-more-events-available? [resource config]
  (not (nil? (hal/get-link resource
               (efc/event-feed-events-next-link-name config)))))

(defn has-not-exceeded-maximum-events-per-poll? [events config]
  (< (count events) (efc/polling-maximum-events-per-poll config)))

(defn load-more-events? [resource events config]
  (and
    (has-not-exceeded-maximum-events-per-poll? events config)
    (has-more-events-available? resource config)))

(defn load-page-of-events
  [navigator link parameters config]
  (let [navigator (halnav/get navigator link parameters)
        resource (halnav/resource navigator)
        events (hal/get-resource resource
                 (efc/event-feed-events-embedded-resource-name config))]
    [navigator resource events]))

(defn load-new-events [config offset]
  (let [discovery-url
        (efc/event-feed-discovery-url config)
        events-per-page
        (efc/event-feed-events-per-page config)
        per-page-template-parameter-name
        (efc/event-feed-per-page-template-parameter-name config)
        since-template-parameter-name
        (efc/event-feed-since-template-parameter-name config)
        discovery-events-link-name
        (efc/event-feed-discovery-events-link-name config)
        events-next-link-name
        (efc/event-feed-events-next-link-name config)
        maximum-events
        (efc/polling-maximum-events-per-poll config)]
    (loop [all-events []
           navigator (halnav/discover discovery-url)
           link discovery-events-link-name
           parameters
           (cond-> {per-page-template-parameter-name events-per-page}
             (not (nil? offset)) (assoc since-template-parameter-name offset))]
      (let [[navigator resource events]
            (load-page-of-events navigator link parameters config)
            all-events (into all-events events)]
        (if (load-more-events? resource all-events config)
          (recur all-events navigator events-next-link-name {})
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
