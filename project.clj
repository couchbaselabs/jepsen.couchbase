(defproject couchbase "0.1.0-SNAPSHOT"
  :description "Jepsen testing for couchbase"
  :main couchbase.core
  :plugins [[lein-cljfmt "0.6.4"]]
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [jepsen "0.1.11"]
                 [com.couchbase.client/java-client "3.0.0-alpha.1"]
                 [com.couchbase.client/dcp-client "0.21.0-SHADEDCORE"]
                 [cheshire "5.8.1"]
                 [clj-http "3.9.1"]]
  :repositories [["couchbase-preview" {:url "https://files.couchbase.com/maven2"
                                       :checksum :ignore}]]
  :jvm-opts ~(if (-> (System/getProperty "java.version")
                     (clojure.string/split #"\.")
                     (first)
                     (Integer/parseInt)
                     (>= 9))
               ["--add-modules" "java.xml.bind"]
               []))
