(defproject io.logicblocks/kafka.connect.event-feed "0.0.1-RC4"
  :description "A Kafka Connect connector to read a HAL based event feed resource."
  :url "https://github.com/logicblocks/kafka.connect.event-feed"

  :license {:name "The MIT License"
            :url  "https://opensource.org/licenses/MIT"}

  :plugins [[lein-cloverage "1.1.2"]
            [lein-shell "0.5.0"]
            [lein-ancient "0.6.15"]
            [lein-changelog "0.3.2"]
            [lein-cprint "1.3.3"]
            [lein-eftest "0.5.9"]
            [lein-codox "0.10.7"]
            [lein-cljfmt "0.6.7"]
            [lein-kibit "0.1.8"]
            [lein-bikeshed "0.5.2"]
            [jonase/eastwood "0.3.11"]]

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.logging "1.1.0"]

                 [halboy "5.1.1"]
                 [json-path "2.1.0"]]

  :profiles
  {:provided
   {:dependencies [[org.apache.kafka/connect-api "2.8.0"]]}
   :shared
   [:provided {:dependencies
               [[org.clojure/test.check "1.1.0"]

                [org.slf4j/jcl-over-slf4j "1.7.30"]
                [org.slf4j/jul-to-slf4j "1.7.30"]
                [org.slf4j/log4j-over-slf4j "1.7.30"]
                [ch.qos.logback/logback-classic "1.2.3"]

                [nrepl "0.8.3"]
                [eftest "0.5.9"]

                [halboy "5.1.1"
                 :exclusions [cheshire
                              org.clojure/core.cache]]

                ; TODO - get rid of these in shared
                [kelveden/clj-wiremock "1.7.0"
                 :exclusions [com.fasterxml.jackson.core/jackson-annotations
                              net.sf.jopt-simple/jopt-simple
                              org.apache.commons/commons-lang3
                              org.apache.httpcomponents/httpcore
                              org.eclipse.jetty/jetty-server
                              org.eclipse.jetty/jetty-servlet
                              org.eclipse.jetty/jetty-servlets
                              riddley]]

                [org.javassist/javassist "3.26.0-GA"]

                [io.logicblocks/kafka.testing "0.0.2"]

                [fundingcircle/jackdaw "0.8.0"
                 :exclusions [org.apache.kafka/kafka-clients]]
                [org.sourcelab/kafka-connect-client "3.1.1"]
                ; TODO - up to here

                [camel-snake-kebab "0.4.2"]
                [uritemplate-clj "1.3.0"]

                [http-kit.fake "0.2.2"]]

               :resource-paths
               ["test-resources"]}]
   :unit
   [:shared {:test-paths ^:replace ["test/shared"
                                    "test/unit"]
             :eftest       {:multithread? false}}]
   :integration
   [:shared {:test-paths   ^:replace ["test/shared"
                                      "test/integration"]
             :eftest       {:multithread? false}}]
   :dev
   [:unit :integration {:test-paths ^:replace ["test/shared"
                                               "test/unit"
                                               "test/integration"]}]

   :uberjar
   {:aot :all}

   :prerelease
   {:release-tasks
    [["shell" "git" "diff" "--exit-code"]
     ["change" "version" "leiningen.release/bump-version" "rc"]
     ["change" "version" "leiningen.release/bump-version" "release"]
     ["vcs" "commit" "Pre-release version %s [skip ci]"]
     ["vcs" "tag"]
     ["deploy"]]}
   :release
   {:release-tasks
    [["shell" "git" "diff" "--exit-code"]
     ["change" "version" "leiningen.release/bump-version" "release"]
     ["codox"]
     ["changelog" "release"]
     ["shell" "sed" "-E" "-i.bak" "s/\"[0-9]+\\.[0-9]+\\.[0-9]+\"/\"${:version}\"/g" "README.md"]
     ["shell" "rm" "-f" "README.md.bak"]
     ["shell" "git" "add" "."]
     ["vcs" "commit" "Release version %s [skip ci]"]
     ["vcs" "tag"]
     ["deploy"]
     ["change" "version" "leiningen.release/bump-version" "patch"]
     ["change" "version" "leiningen.release/bump-version" "rc"]
     ["change" "version" "leiningen.release/bump-version" "release"]
     ["vcs" "commit" "Pre-release version %s [skip ci]"]
     ["vcs" "tag"]
     ["vcs" "push"]]}}

  :aot [kafka.connect.event-feed.task
        kafka.connect.event-feed.connector]

  :java-source-paths ["src/java"]
  :source-paths ["src/clojure"]
  :test-paths ["test/shared"
               "test/unit"
               "test/integration"]

  :target-path "target/%s/"

  :javac-options ["-source" "8" "-target" "8"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]

  :cloverage
  {:ns-exclude-regex [#"^user"]}

  :codox
  {:namespaces  [#"^kafka\.connect\.event-feed"]
   :metadata    {:doc/format :markdown}
   :output-path "docs"
   :doc-paths   ["docs"]
   :source-uri  "https://github.com/logicblocks/kafka.connect.event-feed/blob/{version}/{filepath}#L{line}"}

  :cljfmt {:indents ^:replace {#".*" [[:inner 0]]}}

  :eastwood {:config-files ["config/linter.clj"]}

  :deploy-repositories
  {"releases"  {:url "https://repo.clojars.org" :creds :gpg}
   "snapshots" {:url "https://repo.clojars.org" :creds :gpg}}

  :aliases {"test" ["do"
                    ["with-profile" "unit" "eftest" ":all"]
                    ["with-profile" "integration" "eftest" ":all"]]})
