(ns kafka.connect.event-feed.config
  (:import
   [org.apache.kafka.common.config
    ConfigDef
    ConfigDef$Type
    ConfigDef$Importance
    ConfigDef$Width]))

(def config-type-mapping
  {:type/boolean  ConfigDef$Type/BOOLEAN
   :type/string   ConfigDef$Type/STRING
   :type/int      ConfigDef$Type/INT
   :type/short    ConfigDef$Type/SHORT
   :type/long     ConfigDef$Type/LONG
   :type/double   ConfigDef$Type/DOUBLE
   :type/list     ConfigDef$Type/LIST
   :type/class    ConfigDef$Type/CLASS
   :type/password ConfigDef$Type/PASSWORD})

(def config-importance-mapping
  {:importance/high   ConfigDef$Importance/HIGH
   :importance/medium ConfigDef$Importance/MEDIUM
   :importance/low    ConfigDef$Importance/LOW})

(def config-width-mapping
  {:width/none   ConfigDef$Width/NONE
   :width/short  ConfigDef$Width/SHORT
   :width/medium ConfigDef$Width/MEDIUM
   :width/long   ConfigDef$Width/LONG})

(defn config-type [key]
  (get config-type-mapping key))

(defn config-importance [key]
  (get config-importance-mapping key))

(defn config-width [key]
  (get config-width-mapping key))

(defn- config-def []
  (ConfigDef.))

(defn- define
  [config-def
   & {:keys [name type default-value importance documentation]
      :or   {default-value nil}}]
  (if default-value
    (.define config-def name
      (config-type type)
      (config-importance importance)
      documentation)
    (.define config-def name
      (config-type type)
      default-value
      (config-importance importance)
      documentation)))

(defn config-definition []
  (-> (config-def)
    (define
      :name "topic.name"
      :type :type/string
      :importance :importance/high
      :documentation (str "The name of the topic to populate with the "
                       "events in the event feed."))
    (define
      :name "eventfeed.discovery.url"
      :type :type/string
      :importance :importance/high
      :documentation (str "The URL of the discovery resource of the API that "
                       "exposes an event feed."))))
