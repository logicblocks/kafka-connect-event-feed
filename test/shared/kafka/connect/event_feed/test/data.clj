(ns kafka.connect.event-feed.test.data
  (:require
   [clojure.spec.alpha :as spec]
   [clojure.spec.gen.alpha :as gen]))

(defn random-uuid []
  (str (gen/generate (spec/gen uuid?))))

(defn random-event-id []
  (random-uuid))
