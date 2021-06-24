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

(defn load-new-events [config offset]
  (let [discovery-url (:eventfeed.discovery.url config)
        pick (:eventfeed.pick config)]
    (loop [all-events []
           navigator (halnav/discover discovery-url)
           link :events
           parameters
           (cond-> {:pick pick}
             (not (nil? offset)) (assoc :since offset))]
      (let [[navigator resource events] (load-page-of-events
                                          navigator link parameters)
            all-events (into all-events events)]
        (if (load-more-events? resource)
          (recur all-events navigator :next {})
          all-events)))))

(defn event->source-record [config event source-partition]
  (let [topic-name (:topic.name config)
        stream-id (hal/get-property event :streamId)
        record (efr/source-record
                 :k stream-id
                 :source-partition source-partition
                 :source-offset {:offset (hal/get-property event :id)}
                 :topic-name topic-name
                 :value (haljson/resource->map event))]
    record))

(defn events->source-records [config events source-partition]
  (efr/source-records
    (map #(event->source-record config % source-partition) events)))
