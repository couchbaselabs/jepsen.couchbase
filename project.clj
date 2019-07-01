(defproject couchbase "0.1.0-SNAPSHOT"
  :description "Jepsen testing for couchbase"
  :main couchbase.core
  :plugins [[lein-cljfmt "0.6.4"]]
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.cli "0.4.2"]
                 [jepsen "0.1.14"]
                 [io.projectreactor/reactor-core "3.2.1.RELEASE"]
                 [io.projectreactor.addons/reactor-adapter "3.2.0.RELEASE"]
                 [io.projectreactor.addons/reactor-extra "3.2.0.RELEASE"]
                 [io.reactivex/rxjava-reactive-streams "1.2.1"]
                 [cheshire "5.8.1"]
                 [clj-http "3.9.1"]
                 [com.couchbase.client/core-io "2.0.0-alpha.3"]
                 [com.couchbase.client/java-client "3.0.0-alpha.3"]]
  :resource-paths ["./lib/dcp-client-0.23.0-SHADEDCORE.jar"
                   "./lib/couchbase-transactions-1.0.0-alpha.3.jar"]
  :repositories [["couchbase-preview" {:url "https://files.couchbase.com/maven2"
                                       :checksum :ignore}]]
  :jvm-opts ~(if (-> (System/getProperty "java.version")
                     (clojure.string/split #"\.")
                     (first)
                     (Integer/parseInt)
                     (>= 9))
               ["--add-modules" "java.xml.bind"]
               []))
