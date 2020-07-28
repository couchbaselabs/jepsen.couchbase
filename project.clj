(defproject couchbase "0.1.0-SNAPSHOT"
  :description "Jepsen testing for couchbase"
  :main couchbase.core
  :plugins [[lein-cljfmt "0.6.7"]
            [lein-kibit "0.1.8"]
            [jonase/eastwood "0.3.11"]]
  :jvm-opts ["-server"       ; Be more aggressive with optimisations
             "-Xms32G"       ; Set the starting heap size to 32GB and the max heap size to 32GB so that we never
             "-Xmx32G"       ; have to re-size the heap which causes the GC to be used. If we go above 32GB we will
                             ; crash but we shouldn't be using that much memory in a normal situation anyway
             "-XX:+UseG1GC"] ; Use Garbage First (G1) Collector, this should reduce pause time over reducing memory
  :dependencies [[org.clojure/clojure "1.10.0"] ;; Bump back to 1.10.1 after we bump Jepsen to 0.2.0
                 [org.clojure/tools.cli "1.0.194"]
                 [jepsen "0.1.19"]
                 [cheshire "5.10.0"]
                 [clj-http "3.10.1"]
                 [com.couchbase.client/core-io "2.0.4"]
                 [com.couchbase.client/java-client "3.0.3"]
                 [com.couchbase.client/couchbase-transactions "1.0.0-beta.3-SNAPSHOT"]]
  :resource-paths ["./lib/dcp-client-0.23.0-SHADEDCORE.jar"]
  :repositories [["couchbase-snapshot" {:url      "https://oss.sonatype.org/content/repositories/snapshots"
                                        :checksum :ignore}]])
