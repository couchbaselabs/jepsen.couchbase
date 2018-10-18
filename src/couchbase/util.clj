(ns couchbase.util
  (:require [clojure.java.shell :as shell]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [control :as c]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import com.couchbase.client.java.CouchbaseCluster
           com.couchbase.client.java.auth.ClassicAuthenticator))

(defn rest-call
  "Perform a rest api call to the active jepsen control node"
  ([endpoint params] (rest-call c/*host* endpoint params))
  ([target endpoint params]
   (let [uri  (str "http://" target ":8091" endpoint)
         call (if (some? params)
                (shell/sh "curl" "-s" "-u" "Administrator:abc123" uri "-d" params)
                (shell/sh "curl" "-s" "-u" "Administrator:abc123" uri))]
     (if (not= (call :exit) 0)
       (throw+ {:type :rest-fail :error call})
       (:out call)))))

(defn initialise
  "Initialise a new cluster"
  []
  (let [data-path  "%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata"
        index-path "%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata"
        params     (format "data_path=%s&index_path=%s" data-path index-path)]
    (rest-call "/nodes/self/controller/settings" params)
    (rest-call "/nodes/controller/setupServices" "services=kv")
    (rest-call "/settings/web" "username=Administrator&password=abc123&port=SAME")))

(defn add-nodes
  "Add nodes to the cluster"
  [nodes-to-add]
  (doseq [node nodes-to-add]
    (let [params (str "hostname=" node
                      "&user=Administrator"
                      "&password=abc123"
                      "&services=kv")]
      (info "Adding node" node "to cluster")
      (rest-call "/controller/addNode" params))))

(defn rebalance
  "Rebalance the cluster"
  [nodes]
  (let [known-nodes-str (->> nodes
                             (map #(str "ns_1@" %))
                             (str/join ",")
                             (str "knownNodes="))
        get-status      #(rest-call "/pools/default/rebalanceProgress" nil)]
    (rest-call "/controller/rebalance" known-nodes-str)

    (loop [status (get-status)]
      (if (not= status "{\"status\":\"none\"}")
        (do
          (info "Rebalance status" status)
          (Thread/sleep 1000)
          (recur (get-status)))
        (info "Rebalance complete")))))

(defn create-bucket
  "Create the default bucket"
  [replicas]
  (let [params (str "flushEnabled=1&replicaNumber=" replicas
                    "&evictionPolicy=valueOnly&ramQuotaMB=256"
                    "&bucketType=couchbase&name=default"
                    "&authType=sasl&saslPassword=")]
    (rest-call "/pools/default/buckets" params)))

(defn setup-cluster
  "Setup couchbase cluster"
  [test node]
  (info "Creating couchbase cluster from" node)
  (let [nodes        (test :nodes)
        other-nodes  (remove #(= node %) nodes)
        num-vbuckets (test :custom-vbucket-count)
        num-replicas (test :replicas)]
    (initialise)
    (add-nodes other-nodes)
    (if num-vbuckets
      (->> num-vbuckets
           (format "ns_config:set(couchbase_num_vbuckets_default, %s).")
           (rest-call "/diag/eval")))
    (rebalance nodes)
    (println (create-bucket num-replicas))
    (rebalance nodes))
  (rest-call "/settings/autoFailover" "enabled=true&timeout=6&maxCount=2")
  (Thread/sleep 3000)
  (info "Setup complete"))

(defn setup-node
  "Start couchbase on a node"
  []
  (info "Setting up couchbase")
  (c/su (c/exec :mkdir "/opt/couchbase/var/lib/couchbase"))
  (c/su (c/exec :chown :couchbase:couchbase "/opt/couchbase/var/lib/couchbase"))
  (c/su (c/exec :systemctl :start :couchbase-server))

  (while
    (= :not-ready
      (try+
        (rest-call "/pools/default" nil)
        (catch [:type :rest-fail] e
          :not-ready)))
    (info "Waiting for couchbase to start")
    (Thread/sleep 2000)))

(defn teardown
  "Stop the couchbase server instances and delete the data files"
  []
  (info "Tearing down couchbase node")
  (c/su (c/exec :systemctl :stop :couchbase-server))
  (c/su (c/exec :rm :-rf "/opt/couchbase/var/lib/couchbase")))

(defn get-connection
  "Use the couchbase java sdk to create a CouchbaseCluster instance, then open
  the default bucket."
  [test]
  (let [nodes   (test :nodes)
        cluster (CouchbaseCluster/create nodes)
        auth    (-> (new ClassicAuthenticator)
                    (.cluster "Administrator" "abc123")
                    (.bucket "default" ""))
        cluster (.authenticate cluster auth)
;        cluster (.authenticate cluster "Administrator" "abc123")
        bucket  (.openBucket cluster "default")]
    {:cluster cluster :bucket bucket}))

(defn close-connection
  "Close the bucket and disconnect from the cluster"
  [conn]
  (let [cluster (conn :cluster)
        bucket  (conn :bucket)]
    (.close bucket)
    (.disconnect cluster)))
