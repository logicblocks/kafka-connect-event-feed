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

(defn events-path-template
  ([] (events-path-template {}))
  ([{:keys [parameter-names]
     :or   {parameter-names {}}}]
   (let [{:keys [per-page since]
          :or   {per-page :perPage
                 since    :since}}
         parameter-names]
     (str "/events{?" (name per-page) "," (name since) "}"))))
(defn event-path-template [] "/events/{eventId}")

(defn discovery-path [] "/")

(defn events-path
  ([] (events-path {}))
  ([{:keys [parameters]
     :or   {parameters {}}
     :as   options}]
   (populate (events-path-template options)
     parameters)))

(defn discovery-href [base-url]
  (str base-url (discovery-path)))

(defn events-template-href
  ([base-url] (events-template-href base-url {}))
  ([base-url options] (str base-url (events-path-template options))))

(defn events-href
  ([base-url]
   (events-href base-url {}))
  ([base-url {:keys [parameters] :as options}]
   (let [template-path (events-path-template options)]
     (populate (str base-url template-path)
       parameters))))

(defn event-href [base-url event-id]
  (populate (str base-url (event-path-template))
    {:event-id event-id}))

(defn discovery-resource
  ([base-url]
   (discovery-resource base-url {}))
  ([base-url {:keys [events-link]}]
   (let [events-link-name (or (:name events-link) :events)]
     (-> (hal/new-resource (discovery-href base-url))
       (hal/add-link events-link-name
         {:href      (events-template-href base-url events-link)
          :templated true})))))

(defn events-resource
  ([base-url & {:keys [events-link
                       next-link
                       event-resources]}]
   (let [next-link-name (or (:name next-link) :next)]
     (-> (hal/new-resource (events-href base-url events-link))
       (hal/add-href :discovery (discovery-href base-url))
       (hal/add-link :events
         (map (fn [event-resource]
                {:href (hal/get-href event-resource :self)})
           event-resources))
       (hal/add-link next-link-name
         (when (not (nil? next-link))
           (events-href base-url next-link)))
       (hal/add-resource :events event-resources)))))

(defn event-resource
  ([base-url] (event-resource base-url {}))
  ([base-url {:keys [id streamId type payload]
              :or   {id       (td/random-event-id)
                     streamId (td/random-stream-id)
                     type     :event-type
                     payload  {}}}]
   (-> (hal/new-resource (event-href base-url id))
     (hal/add-link :discovery (discovery-href base-url))
     (hal/add-properties
       {:id       id
        :streamId streamId
        :type     (name type)
        :payload  payload}))))
