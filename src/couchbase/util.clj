(ns couchbase.util
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [control :as c]]
            [slingshot.slingshot :refer [try+]])
  (:import com.couchbase.client.java.CouchbaseCluster))

(defn rest-call
  "Perform a rest api call"
  [endpoint params]
  (let [uri (str "http://localhost:8091" endpoint)]
    (if (some? params)
      (c/exec :curl :-s :-u :Administrator:abc123 uri :-d params)
      (c/exec :curl :-s :-u :Administrator:abc123 uri))))

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
        status          #(rest-call "/pools/default/rebalanceProgress" nil)]
    (rest-call "/controller/rebalance" known-nodes-str)
    (while (not= (status) "{\"status\":\"none\"}")
      (info "Rebalance status" (status))
      (Thread/sleep 250))))

(defn create-bucket
  "Create the default bucket"
  [replicas]
  (let [params (str "flushEnabled=1&replicaNumber=" replicas
                    "&evictionPolicy=valueOnly&ramQuotaMB=256"
                    "&bucketType=couchbase&name=default")]
    (rest-call "/pools/default/buckets" params)))

(defn setup-cluster
  "Setup couchbase cluster"
  [test node]
  (info "Creating couchbase cluster from" node)
  (let [nodes        (test :nodes)
        other-nodes  (remove #(= node %) nodes)
        num-replicas (test :replicas)]
    (initialise)
    (add-nodes other-nodes)
    (create-bucket num-replicas)
    (rebalance nodes))
  (rest-call "/settings/autoFailover" "enabled=true&timeout=6&maxCount=2")
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
        (catch Exception e
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
          cluster (.authenticate cluster "Administrator" "abc123")
          bucket  (.openBucket cluster "default")]
    {:cluster cluster :bucket bucket}))

(defn close-connection
  "Close the bucket and disconnect from the cluster"
  [conn]
  (let [cluster (conn :cluster)
        bucket  (conn :bucket)]
    (.close bucket)
    (.disconnect cluster)))
