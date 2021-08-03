(ns kafka.connect.event-feed.config
  (:require
   [kafka.connect.event-feed.utils :as efu])
  (:import
   [org.apache.kafka.common.config
    AbstractConfig
    ConfigDef
    ConfigDef$Type
    ConfigDef$Importance
    ConfigDef$Width]
   [java.util Map]))

(def configuration-type-mapping
  {:type/boolean  ConfigDef$Type/BOOLEAN
   :type/string   ConfigDef$Type/STRING
   :type/int      ConfigDef$Type/INT
   :type/short    ConfigDef$Type/SHORT
   :type/long     ConfigDef$Type/LONG
   :type/double   ConfigDef$Type/DOUBLE
   :type/list     ConfigDef$Type/LIST
   :type/class    ConfigDef$Type/CLASS
   :type/password ConfigDef$Type/PASSWORD})

(def configuration-importance-mapping
  {:importance/high   ConfigDef$Importance/HIGH
   :importance/medium ConfigDef$Importance/MEDIUM
   :importance/low    ConfigDef$Importance/LOW})

(def configuration-width-mapping
  {:width/none   ConfigDef$Width/NONE
   :width/short  ConfigDef$Width/SHORT
   :width/medium ConfigDef$Width/MEDIUM
   :width/long   ConfigDef$Width/LONG})

(defn configuration-type [k]
  (get configuration-type-mapping k))

(defn configuration-importance [k]
  (get configuration-importance-mapping k))

(defn configuration-width [k]
  (get configuration-width-mapping k))

(defn- config-def []
  (ConfigDef.))

(defn- define
  [^ConfigDef config-def
   & {:keys [name type default-value importance documentation]
      :or   {default-value nil}}]
  (if default-value
    (.define config-def name
      (configuration-type type)
      default-value
      (configuration-importance importance)
      documentation)
    (.define config-def name
      (configuration-type type)
      (configuration-importance importance)
      documentation)))

(defn configuration-definition []
  (-> (config-def)
    (define
      :name "topic.name"
      :type :type/string
      :importance :importance/high
      :documentation (str "The name of the topic to populate with the "
                       "events in the event feed."))
    (define
      :name "polling.interval.ms"
      :type :type/long
      :default-value 200
      :importance :importance/medium
      :documentation (str "The number of milliseconds to wait between "
                       "attempts to fetch new events from the event feed."))
    (define
      :name "polling.max.events.per.poll"
      :type :type/long
      :default-value 1000
      :importance :importance/medium
      :documentation (str "The maximum number of events to consumed during "
                       "an attempt to fetch new events from the event feed."))
    (define
      :name "eventfeed.discovery.url"
      :type :type/string
      :importance :importance/high
      :documentation (str "The URL of the discovery resource of the API that "
                       "exposes an event feed."))
    (define
      :name "eventfeed.events.per.page"
      :type :type/int
      :importance :importance/medium
      :documentation (str "The number of events to request in each request to "
                       "the event feed."))
    (define
      :name "eventfeed.template.parameter.name.per.page"
      :type :type/string
      :default-value "perPage"
      :importance :importance/medium
      :documentation (str "The name of the template parameter in the "
                       "templated events link that determines the number of "
                       "events to fetch per page."))
    (define
      :name "eventfeed.template.parameter.name.since"
      :type :type/string
      :default-value "since"
      :importance :importance/medium
      :documentation (str "The name of the template parameter in the "
                       "templated events link that determines the identifier "
                       "of the last event that was seen."))
    (define
      :name "eventfeed.link.name.discovery.events"
      :type :type/string
      :default-value "events"
      :importance :importance/medium
      :documentation (str "The name of the events link in the discovery "
                       "resource of the API that exposes an event feed."))
    (define
      :name "eventfeed.link.name.events.next"
      :type :type/string
      :default-value "next"
      :importance :importance/medium
      :documentation (str "The name of the next link in the events "
                       "resource of the API that exposes an event feed."))
    (define
      :name "eventfeed.embedded.resource.name.events"
      :type :type/string
      :default-value "events"
      :importance :importance/medium
      :documentation (str "The name of the embedded resource that contains "
                       "events in the events resource of the API that exposes "
                       "an event feed."))
    (define
      :name "events.fields.offset.jsonpath"
      :type :type/string
      :default-value "$.id"
      :importance :importance/medium
      :documentation "A JSONPath to the field to use as the event offset.")
    (define
      :name "events.fields.key.jsonpath"
      :type :type/string
      :default-value "$.streamId"
      :importance :importance/medium
      :documentation (str "A JSONPath to the field to use as the event key, "
                       "useful for partitioning retrieved events."))))

(defn configuration [^Map properties]
  (-> (configuration-definition)
    (AbstractConfig. properties)
    (.values)
    (efu/java-data->clojure-data)
    (assoc :connector.name (get properties "name"))))

(defn connector-name [config]
  (:connector.name config))

(defn topic-name [config]
  (:topic.name config))

(defn polling-fetch-interval-milliseconds [config]
  (:polling.interval.ms config))

(defn polling-maximum-events-per-poll [config]
  (let [maximum-events (:polling.max.events.per.poll config)]
    (if (neg? maximum-events) (Long/MAX_VALUE) maximum-events)))

(defn event-feed-discovery-url [config]
  (:eventfeed.discovery.url config))

(defn event-feed-events-per-page [config]
  (:eventfeed.events.per.page config))

(defn event-feed-per-page-template-parameter-name [config]
  (keyword (:eventfeed.template.parameter.name.per.page config)))

(defn event-feed-since-template-parameter-name [config]
  (keyword (:eventfeed.template.parameter.name.since config)))

(defn event-feed-discovery-events-link-name [config]
  (keyword (:eventfeed.link.name.discovery.events config)))

(defn event-feed-events-next-link-name [config]
  (keyword (:eventfeed.link.name.events.next config)))

(defn event-feed-events-embedded-resource-name [config]
  (keyword (:eventfeed.embedded.resource.name.events config)))

(defn event-offset-field-jsonpath [config]
  (:events.fields.offset.jsonpath config))

(defn event-key-field-jsonpath [config]
  (:events.fields.key.jsonpath config))
