(defproject couchbase "0.1.0-SNAPSHOT"
  :description "Jepsen testing for couchbase"
  :main couchbase.core
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [jepsen "0.1.11"]
                 [com.couchbase.client/java-client "2.6.2"]
                 [com.couchbase.client/dcp-client "0.20.0"]
                 [cheshire "5.8.1"]
                 [clj-http "3.9.1"]]
  :jvm-opts ~(if (-> (System/getProperty "java.version")
                     (clojure.string/split #"\.")
                     (first)
                     (Integer/parseInt)
                     (>= 9))
               ["--add-modules" "java.xml.bind"]
               []))
