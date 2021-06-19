(ns kafka.connect.event-feed.events
  (:require
   [halboy.navigator :as halnav]
   [halboy.resource :as hal]
   [halboy.json :as haljson]

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

(defn load-new-events [config]
  (let [discovery-url (:eventfeed.discovery.url config)
        pick (:eventfeed.pick config)]
    (loop [all-events []
           navigator (halnav/discover discovery-url)
           link :events
           parameters {:pick pick}]
      (let [[navigator resource events] (load-page-of-events
                                          navigator link parameters)
            all-events (into all-events events)]
        (if (load-more-events? resource)
          (recur all-events navigator :next {})
          all-events)))))

(defn event->source-record [config event]
  (let [topic-name (:topic.name config)]
    (efr/source-record
      :topic-name topic-name
      :value (haljson/resource->map event))))

(defn events->source-records [config events]
  (efr/source-records
    (map #(event->source-record config %) events)))
