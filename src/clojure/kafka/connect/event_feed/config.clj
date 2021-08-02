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
  [config-def
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
      :name "eventfeed.pagination"
      :type :type/boolean
      :default-value true
      :importance :importance/medium
      :documentation (str "Defines whether poll returns only one page or"
                       "as far as it can go"))
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
  (:polling.max.events.per.poll config))

(defn event-feed-discovery-url [config]
  (:eventfeed.discovery.url config))

(defn event-feed-events-per-page [config]
  (:eventfeed.events.per.page config))

(defn event-feed-pagination [config]
  (:eventfeed.pagination config))

(defn event-offset-field-jsonpath [config]
  (:events.fields.offset.jsonpath config))

(defn event-key-field-jsonpath [config]
  (:events.fields.key.jsonpath config))
