(defproject couchbase "0.1.0-SNAPSHOT"
  :description "Jepsen testing for couchbase"
  :main couchbase.core
  :plugins [[lein-cljfmt "0.6.4"]
            [lein-kibit "0.1.7"]
            [jonase/eastwood "0.3.5"]]
  :jvm-opts ["-server"       ; Be more aggressive with optimisations
             "-Xms32G"       ; Set the starting heap size to 32GB and the max heap size to 32GB so that we never
             "-Xmx32G"       ; have to re-size the heap which causes the GC to be used. If we go above 32GB we will
                             ; crash but we shouldn't be using that much memory in a normal situation anyway
             "-XX:+UseG1GC"] ; Use Garbage First (G1) Collector, this should reduce pause time over reducing memory
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.cli "0.4.2"]
                 [jepsen "0.1.15"]
                 [cheshire "5.9.0"]
                 [clj-http "3.10.0"]
                 [com.couchbase.client/core-io "2.0.0-beta.1"]
                 [com.couchbase.client/java-client "3.0.0-beta.1"]
                 [com.couchbase.client/couchbase-transactions "1.0.0-beta.3-SNAPSHOT"]]
  :resource-paths ["./lib/dcp-client-0.23.0-SHADEDCORE.jar"]
  :repositories [["couchbase-preview" {:url      "https://files.couchbase.com/maven2"
                                       :checksum :ignore}]
                 ["couchbase-snapshot" {:url "https://oss.sonatype.org/content/repositories/snapshots"
                                        :checksum :ignore}]])