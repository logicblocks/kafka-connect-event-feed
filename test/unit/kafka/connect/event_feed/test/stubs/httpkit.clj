(ns kafka.connect.event-feed.test.stubs.httpkit
  (:require
   [halboy.json :as haljson]

   [org.bovinegenius.exploding-fish :as uri]

   [kafka.connect.event-feed.test.resources :as tr]))

(defn discovery-resource [base-url]
  [{:method :get :url (tr/discovery-href base-url)}
   {:status 200
    :body   (haljson/resource->json
              (tr/discovery-resource base-url))}])

(defn events-resource
  [base-url & {:keys [events-link-parameters
                      next-link-parameters
                      event-resources]}]
  (let [events-url (tr/events-href base-url events-link-parameters)
        events-url-without-query-params (uri/query events-url nil)
        events-url-query-params (uri/query-map events-url)]
    [{:method       :get
      :url          events-url-without-query-params
      :query-params events-url-query-params}
     {:status 200
      :body   (haljson/resource->json
                (tr/events-resource base-url
                  :events-link-parameters events-link-parameters
                  :next-link-parameters next-link-parameters
                  :event-resources event-resources))}]))
