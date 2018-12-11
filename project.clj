(defproject couchbase "0.1.0-SNAPSHOT"
  :description "Jepsen testing for couchbase"
  :main couchbase.core
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [jepsen "0.1.10"]
                 [com.couchbase.client/java-client "2.6.2"]
                 [com.couchbase.client/dcp-client "0.20.0"]]
  :jvm-opts ["--add-modules" "java.xml.bind"])
