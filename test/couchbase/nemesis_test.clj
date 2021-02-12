(ns couchbase.nemesis-test
  (:require [clojure.test :refer :all]
            [couchbase.nemesis :as nemesis]))

(deftest targeter-test
  (testing "target-first-node"
    (is (= ["172.28.128.20"]
           (nemesis/target-first-node {:nodes ["172.28.128.20" "172.28.128.21" "172.28.128.22"]} nil))))
  (testing "basic-nodes-targeter"
    (is (= ["172.28.128.20"]
           (nemesis/basic-nodes-targeter {:nodes ["172.28.128.20"]} nil))))
  (testing "basic-nodes-targeter-all-values"
    (is (= ["172.28.128.20" "172.28.128.21" "172.28.128.22"]
           (sort (nemesis/basic-nodes-targeter {:nodes ["172.28.128.20" "172.28.128.21" "172.28.128.22"]}
                                               {:target-count 3}))))))
