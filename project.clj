(defproject io.logicblocks/kafka.connect.event-feed "0.1.0-RC7"
  :description "A Kafka Connect connector to read a HAL based event feed resource."
  :url "https://github.com/logicblocks/kafka.connect.event-feed"

  :license {:name "The MIT License"
            :url  "https://opensource.org/licenses/MIT"}

  :plugins [[lein-cloverage "1.2.4"]
            [lein-shell "0.5.0"]
            [lein-ancient "0.7.0"]
            [lein-changelog "0.3.2"]
            [lein-cprint "1.3.3"]
            [lein-eftest "0.6.0"]
            [lein-codox "0.10.8"]
            [lein-cljfmt "0.9.2"]
            [lein-kibit "0.1.8"]
            [lein-bikeshed "0.5.2"]
            [lein-ver "1.1.0"]
            [jonase/eastwood "1.4.0"]]

  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/tools.logging "1.2.4"]
                 [org.clojure/core.cache "1.0.225"]

                 [io.logicblocks/halboy "6.0.0"]
                 [json-path "2.2.0"]]

  :profiles
  {:provided
   {:dependencies [[org.apache.kafka/connect-api "3.6.1"]]}

   :shared
   ^{:pom-scope :test}
   [:provided {:dependencies
               [[org.clojure/test.check "1.1.1"]

                [org.slf4j/jcl-over-slf4j "1.7.30"]
                [org.slf4j/jul-to-slf4j "1.7.30"]
                [org.slf4j/log4j-over-slf4j "1.7.30"]
                [ch.qos.logback/logback-classic "1.2.3"]

                [nrepl "1.1.0"]
                [eftest "0.6.0"]

                [io.logicblocks/halboy "6.0.0"]

                [camel-snake-kebab "0.4.3"]
                [uritemplate-clj "1.3.1"]
                [org.bovinegenius/exploding-fish "0.3.6"]]

               :resource-paths
               ["test-resources"]}]

   :unit
   ^{:pom-scope :test}
   [:shared {:dependencies
             [[http-kit.fake "0.2.2"]]

             :test-paths
             ^:replace ["test/shared"
                        "test/unit"]

             :eftest
             {:multithread? false}}]

   :integration
   ^{:pom-scope :test}
   [:shared {:dependencies
             [[kelveden/clj-wiremock "1.8.0"
               :exclusions [com.fasterxml.jackson.core/jackson-annotations
                            net.sf.jopt-simple/jopt-simple
                            org.apache.commons/commons-lang3
                            org.apache.httpcomponents/httpcore
                            org.eclipse.jetty/jetty-proxy
                            org.eclipse.jetty/jetty-server
                            org.eclipse.jetty/jetty-servlet
                            org.eclipse.jetty/jetty-servlets
                            org.eclipse.jetty/jetty-webapp
                            riddley]]

              [org.javassist/javassist "3.26.0-GA"]

              [io.logicblocks/kafka.testing "0.0.3-RC5"]

              [org.apache.kafka/kafka-clients "3.6.1"]
              [org.apache.kafka/kafka-streams "3.6.1"]
              [org.apache.kafka/kafka-streams-test-utils "3.6.1"]

              [fundingcircle/jackdaw "0.9.12"]
              [org.sourcelab/kafka-connect-client "4.0.3"]]

             :test-paths
             ^:replace ["test/shared"
                        "test/integration"]

             :eftest
             {:multithread? false}}]

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
