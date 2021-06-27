(ns kafka.connect.event-feed.test.resources
  (:require
   [clojure.walk :as w]

   [halboy.resource :as hal]

   [camel-snake-kebab.core :as csk]
   [camel-snake-kebab.extras :as cske]

   [uritemplate-clj.core :as uritmpl]
   [kafka.connect.event-feed.test.data :as td]))

(defn populate [uri-template params]
  (let [params (cske/transform-keys csk/->camelCaseString params)]
    (uritmpl/uritemplate uri-template params)))

(defn events-path-template [] "/events{?since,pick}")
(defn event-path-template [] "/events/{eventId}")

(defn discovery-path [] "/")

(defn events-path
  ([] (events-path {}))
  ([params]
   (populate (events-path-template)
     params)))

(defn discovery-href [base-url]
  (str base-url (discovery-path)))

(defn events-template-href [base-url]
  (str base-url (events-path-template)))

(defn events-href
  ([base-url]
   (events-href base-url {}))
  ([base-url params]
   (populate (str base-url (events-path-template))
     params)))

(defn event-href [base-url event-id]
  (populate (str base-url (event-path-template))
    {:event-id event-id}))

(defn discovery-resource [base-url]
  (-> (hal/new-resource (discovery-href base-url))
    (hal/add-link :events
      {:href      (events-template-href base-url)
       :templated true})))

(defn events-resource
  ([base-url & {:keys [events-link-parameters
                       next-link-parameters
                       event-resources]}]
   (-> (hal/new-resource (events-href base-url events-link-parameters))
     (hal/add-href :discovery (discovery-href base-url))
     (hal/add-link :events
       (map (fn [event-resource]
              {:href (hal/get-href event-resource :self)})
         event-resources))
     (hal/add-link :next
       (when (not (nil? next-link-parameters))
         (events-href base-url next-link-parameters)))
     (hal/add-resource :events event-resources))))

(defn event-resource
  [base-url {:keys [id streamId type payload]
             :or   {id        (td/random-event-id)
                    streamId (td/random-stream-id)
                    type      :event-type
                    payload   {}}}]
  (-> (hal/new-resource (event-href base-url id))
    (hal/add-link :discovery (discovery-href base-url))
    (hal/add-properties
      {:id       id
       :streamId streamId
       :type     (name type)
       :payload  payload})))
