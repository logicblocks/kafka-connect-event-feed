(ns clj-wiremock.fixtures
  (:require
   [clj-wiremock.core :as wmc])
  (:import [java.net ServerSocket]))

(defn- free-port! []
  (with-open [socket (ServerSocket. 0)]
    (.getLocalPort socket)))

(defn with-wiremock [wiremock-atom]
  (fn [run-tests]
    (let [port (free-port!)
          fixture-fn (wmc/wiremock-fixture-fn [{:port port}])]
      (fixture-fn
        (fn []
          (try
            (reset! wiremock-atom (wmc/server port))
            (run-tests)
            (finally
              (reset! wiremock-atom nil))))))))
