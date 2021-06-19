(ns clj-wiremock.utils
  (:require
   [clj-wiremock.server :as wms]))

(defn base-url [s]
  (wms/url s ""))
