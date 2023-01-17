(defproject couchbase "0.1.0-SNAPSHOT"
  :description "Jepsen testing for couchbase"
  :main couchbase.core
  :plugins [[lein-cljfmt "0.9.0"]
            [lein-kibit "0.1.8"]
            [jonase/eastwood "0.3.12"]]
  :jvm-opts ["-server"       ; Be more aggressive with optimisations
             "-Xms32G"       ; Set the starting heap size to 32GB and the max heap size to 32GB so that we never
             "-Xmx32G"       ; have to re-size the heap which causes the GC to be used. If we go above 32GB we will
                             ; crash but we shouldn't be using that much memory in a normal situation anyway
             "-XX:+UseG1GC"] ; Use Garbage First (G1) Collector, this should reduce pause time over reducing memory
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/tools.cli "1.0.214"]
                 [org.slf4j/slf4j-simple "2.0.6"]
                 [jepsen "0.3.1"]
                 [cheshire "5.11.0"]
                 [clj-http "3.12.3"]
                 [com.couchbase.client/core-io "2.4.0"]
                 [com.couchbase.client/java-client "3.4.0"]
                 [com.couchbase.client/dcp-client "0.42.0"]]
  :profiles {:dev {:resource-paths ["./test/couchbase/resources"]}
             :uberjar {:aot :all}}
  :repositories [["couchbase-snapshot" {:url      "https://oss.sonatype.org/content/repositories/snapshots"
                                        :checksum :ignore}]])
