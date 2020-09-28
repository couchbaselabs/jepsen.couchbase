(ns couchbase.cbjcli
  (:require [clojure.tools.logging :refer [info warn error fatal]]
            [couchbase.util :as util]
            [clojure.string :as str])
  (:import (java.time Duration)))

; GLOBALS
(def ^:dynamic *use-subdoc* false)
(def ^:dynamic *padding-size* 0)

(defn parse-int
  "Function to check a value can be converted to an int"
  [x]
  (Integer/parseInt x))

(def extra-cli-options
  [[nil "--package URL-OR-FILENAME"
    "Install this couchbase package, use pre-installed version if not given"
    :parse-fn util/get-package]
   [nil "--install-path PATH"
    "The path of the couchbase install on the nodes"
    :default-fn (fn [opts] (or (-> opts :package :path) "/opt/couchbase"))]
   [nil "--workload WORKLOAD"
    "The workload to run"]
   [nil "--oplimit LIMIT"
    "Limit the total number of operations"
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--[no-]cbcollect"
    "Generate a cbcollect at the end of the run?"
    :default true]
   [nil "--perf-graphs"
    "Output performance graphs? (Requires gnuplot)"
    :default false]
   [nil "--hashdump"
    "Output hashtable dump from all vbuckets"
    :default false]
   [nil "--replicas REPLICAS"
    "Number of replicas"
    :default 1
    :parse-fn parse-int]
   [nil "--replicate-to REPLICATE-TO"
    "Observe based durability replicate-to value"
    :parse-fn parse-int
    :default 0]
   [nil "--persist-to PERSIST-TO"
    "Observe based durability persist-to value"
    :parse-fn parse-int
    :default 0]
   [nil "--rate RATE"
    "Rate of operations. A rate of 0 disables rate limiting"
    :parse-fn read-string
    :validate [#(and (number? %) (not (neg? %))) "Must be a non-negative number"]]
   [nil "--pool-size POOL-SIZE"
    "Number of Couchbase SDK client connection pools to create"
    :default 6
    :parse-fn parse-int
    :validate [pos? "Must be a positive number"]]
   [nil "--[no-]autofailover"
    "Enable autofailover?"]
   [nil "--[no-]server-group-autofailover"
    "Enable server group autofailover"]
   [nil "--[no-]disk-autofailover"
    "Enable disk autofailover?"]
   [nil "--autofailover-timeout AUTOFAILOVER-TIMEOUT"
    "Autofailover timeout if autofailover is enabled"
    :parse-fn parse-int
    :validate [#(> % 5) "Must be greater than 5 seconds"]]
   [nil "--disk-autofailover-timeout DISK-AUTOFAILOVER-TIMEOUT"
    "Autofailover timeout if autofailover is enabled"
    :parse-fn parse-int
    :validate [#(> % 5) "Must be greater than 5 seconds"]]
   [nil "--autofailover-maxcount AUTOFAILOVER-MAXCOUNT"
    "Autofailover max count if autofailover is enabled"
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--[no-]autoreprovision"
    "Enable autoreprovisioning for ephemeral buckets?"
    :default true]
   [nil "--autoreprovision-maxnodes AUTOREPROVISION-MAXNODES"
    "Autoreprovision max nodes for ephemeral buckets if autoreprovisioning is enabled"
    :parse-fn parse-int
    :default 1]
   [nil "--doc-count DOC-COUNT"
    "Number of documents"
    :default 30
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--doc-threads DOC-THREADS"
    "Number of threads per document"
    :default 3
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--recovery-type RECOVERY-TYPE"
    :default :delta
    :parse-fn #(cond (= % "delta") :delta (= % "full") :full :else :invalid)
    :validate [#(not= :invalid %) "Must be delta or full"]]
   [nil "--failover-type FAILOVER-TYPE"
    :parse-fn #(cond (= % "hard") :hard (= % "graceful") :graceful :else :invalid)
    :validate [#(not= :invalid %) "Must be hard or graceful"]]
   [nil "--disrupt-count COUNT"
    "Number of nodes to disrupt"
    :default 1
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--skip-teardown"
    "Skip teardown of Couchbase server"
    :default false]
   [nil "--bucket-type TYPE"
    "Type of bucket to create (persistent or ephemeral)"
    :parse-fn {"persistent" :couchbase "ephemeral" :ephemeral}
    :validate [some? "Bucket type must be 'persistent' or 'ephemeral'"]
    :default :couchbase]
   [nil "--server-groups-enabled"
    "Turn on server groups"
    :default false]
   [nil "--target-server-groups"
    "Nemesis will target server groups"
    :default false]
   [nil "--cycles CYCLES"
    "Number of nemesis cycles to run"
    :default 1
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--disrupt-time DISRUPTION-TIME"
    "Number of seconds for which the nemesis will act"
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--connect-timeout CONNECT-TIMEOUT"
    "Number of second what the java client will wait when trying to connect to Couchbase Server"
    :parse-fn parse-int
    :valid? [#(and (number? %) (pos? %)) "Must be a positive int"]
    :default 10]
   [nil "--kv-timeout KV-TIMEOUT"
    "Timeout for kv operations before aborting with an ambiguous response"
    :parse-fn #(->> % (Double/parseDouble) (* 1000) (Duration/ofMillis))
    :default (Duration/ofSeconds 2.5)]
   [nil "--node-count NODE-COUNT"
    "Number of nodes to use for this test"
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--server-group-count SERVER-GROUP-COUNT"
    "Number of nodes to use for this test"
    :default 1
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--scenario SCENARIO"
    :parse-fn keyword]
   [nil "--durability L0:L1:L2:L3"
    "Probability distribution for the durability level"
    :default [100 0 0 0]
    :parse-fn #(map parse-int (str/split % #":"))
    :validate [#(and (= (reduce + %) 100)
                     (= (count %) 4))]]
   [nil "--custom-vbucket-count CUSTOM-VBUCKET-COUNT"
    :parse-fn parse-int
    :validate [#(<= 1 % 1024) "Vbucket count must be between 1 and 1024"]]
   [nil "--cas"
    "Enable CAS operations"
    :default false]
   [nil "--eviction-policy EVICTION-POLICY"
    "Eviction policy for the bucket"
    :parse-fn {"full" "fullEviction" "value" "valueOnly"}
    :default "fullEviction"]
   [nil "--manipulate-disks"
    "Turn on the ability to inject disk failures"
    :default false]
   [nil "--transactions"
    "Turn on the multi-document transactions"
    :default false]
   [nil "--mixed-txns"
    "Allow individual transactions to contain both reads and write"
    :default false]
   [nil "--enable-memcached-debug-log-level"
    "Set memcached log level to debug on all nodes"
    :default false]
   [nil "--enable-tcp-capture"
    "Enable the tcp packet capture on eth1 for Couchbase Server running on a VM use a vagrant and is not supported for --cluster-run"
    :default false]
   [nil "--net-interface INTERFACE"
    "Name of the interface to perform packet capture on by default this is eth1"
    :default "eth1"]
   [nil "--cluster-run"
    "Start a cluster-run of the provided package on the host rather than using provided nodes"
    :default false]
   [nil "--dcp-set-read"
    "Use DCP to read back set keys"
    :default false]
   [nil "--collect-data-files"
    "Use to enable the collection of Couchbase-Servers data directory"
    :default false]
   [nil "--collect-core-files"
    "Use to enable the collection of any core files created during the test"
    :default false]
   [nil "--process-suspend-time TIME"
    "Use to set the number of seconds we should halt a process for"
    :default 10
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--process-to-suspend PROCESS"
    "Use to specify the name of the process to halt and then continue during a kill or set-kill workload"
    :parse-fn {"memcached" :memcached "ns-server" :ns-server "babysitter" :babysitter}
    :validate [some? "Bucket type must be 'memcached', 'ns-server' or 'babysitter'"]]
   [nil "--use-json-docs"
    :default false]
   [nil "--doc-padding-size PADDING-IN-KB"
    "Amount of padding in KB that should be added to the document when performing a write. This is to help create data > memory scenarios"
    :parse-fn #(alter-var-root #'*padding-size* (constantly (parse-int %)))
    :validate [#(and (number? %) (pos? %)) "Must be a number"]]
   [nil "--use-subdoc"
    "Use specify if subdoc mutations should be used"
    :parse-fn (fn [_] (alter-var-root #'*use-subdoc* (constantly true)))
    :default false]
   [nil "--disable-auto-compaction"
    "Use to disable auto-compaction"
    :default false]
   [nil "--disable-out-of-order-execution"
    "Use to disable out of order execution by Couchbase server"
    :default false]])
