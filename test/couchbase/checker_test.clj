(ns couchbase.checker_test
  (:refer-clojure :exclude [set])
  (:use clojure.test)
  (:require [jepsen.checker :as checker]
            [couchbase.checker :refer :all]
            [knossos
             [core :refer [ok-op invoke-op fail-op]]]))

(deftest set-upsert-checker-test

  ;;In the history we have 'value' to denote the set (to be added or upserted), and
  ;;insert-value to denote the value to be added(or upserted) to that set.

  (testing "add-roll-backs"
    (is (= (checker/check (set-upsert-checker) nil
                          [{:value 1, :time 1, :process 1,  :type :invoke, :f :add, :insert-value 1}
                           {:value 1, :time 2, :process 2,  :type :invoke, :f :upsert, :insert-value 2}
                           {:value 1, :time 3, :process 2,  :type :ok, :f :upsert, :insert-value 2}
                           {:value 2, :time 4, :process 3,  :type :invoke, :f :add, :insert-value 2}
                           {:value 2, :time 5, :process 3,  :type :ok, :f :add, :insert-value 2}
                           {:value 2, :time 6, :process 1,  :type :invoke, :f :upsert, :insert-value 3}
                           {:value 2, :time 7, :process 1,  :type :ok, :f :upsert, :insert-value 3}
                           {:value 3, :time 8, :process 2,  :type :invoke, :f :add, :insert-value 3}
                           {:value 3, :time 9, :process 2,  :type :ok, :f :add, :insert-value 3}
                           {:value 3, :time 10, :process 3,  :type :invoke, :f :upsert, :insert-value 4}
                           {:value 3, :time 11, :process 3,  :type :ok, :f :upsert, :insert-value 4}
                           {:value nil, :time 12, :process 1,  :type :invoke, :f :read}
                           {:value [[1 2] [2 3] [3 4]], :time 13, :process 1,  :type :ok, :f :read}]
                          ;;key 1 was not committed for add operation. So read for key 3 should give
                          ;;it's upserted value which is 4.
                          {})
           {:valid?              true
            :add-attempt-count     3
            :upsert-attempt-count 3
            :add-acknowledged-count  2
            :upsert-acknowledged-count  3
            :ok-count            3
            :lost-count          0
            :recovered-count     1
            :unexpected-count    0
            :upsert-not-rolled-back-count  0
            :add-not-rolled-back-count     0
            :ok                 "#{1..3}"
            :lost                "#{}"
            :unexpected           "#{}"
            :upsert-not-rolled-back   "#{}"
            :add-not-rolled-back  "#{}"
            :upsert-rolled-back    "#{}"
            :add-rolled-back   "#{1}"
            :recovered            "#{1}"})))

  (testing "upsert-roll-backs"
    (is (= (checker/check (set-upsert-checker) nil
                          [{:value 1, :time 1, :process 1,  :type :invoke, :f :add, :insert-value 1}
                           {:value 1, :time 2, :process 1,  :type :ok, :f :add, :insert-value 1}
                           {:value 1, :time 3, :process 2,  :type :invoke, :f :upsert, :insert-value 2}
                           {:value 1, :time 4, :process 2,  :type :ok, :f :upsert, :insert-value 2}
                           {:value 2, :time 5, :process 3,  :type :invoke, :f :add, :insert-value 2}
                           {:value 2, :time 6, :process 3,  :type :ok, :f :add, :insert-value 2}
                           {:value 2, :time 7, :process 1,  :type :invoke, :f :upsert, :insert-value 3}
                           {:value 2, :time 8, :process 1,  :type :ok, :f :upsert, :insert-value 3}
                           {:value 3, :time 9, :process 2,  :type :invoke, :f :add, :insert-value 3}
                           {:value 3, :time 10, :process 2,  :type :ok, :f :add, :insert-value 3}
                           {:value 3, :time 11, :process 3,  :type :invoke, :f :upsert, :insert-value 4}
                           {:value nil, :time 12, :process 1,  :type :invoke, :f :read}
                           {:value [[1 2] [2 3] [3 3]], :time 13, :process 1,  :type :ok, :f :read}]
                          ;;key 3 was not committed when upserted. So read for key 3 should be the insert-value of
                          ;;add operation which is 3.
                          {})
           {:valid?              true
            :add-attempt-count     3
            :upsert-attempt-count 3
            :add-acknowledged-count  3
            :upsert-acknowledged-count  2
            :ok-count            3
            :lost-count          0
            :recovered-count     0
            :unexpected-count    0
            :upsert-not-rolled-back-count  0
            :add-not-rolled-back-count     0
            :ok                 "#{1..3}"
            :lost                "#{}"
            :unexpected           "#{}"
            :upsert-not-rolled-back   "#{}"
            :add-not-rolled-back  "#{}"
            :upsert-rolled-back    "#{3}"
            :add-rolled-back   "#{}"
            :recovered            "#{}"})))

  (testing "lost"
    (is (= (checker/check (set-upsert-checker) nil
                          [{:value 1, :time 1, :process 1,  :type :invoke, :f :add, :insert-value 1}
                           {:value 1, :time 2, :process 1,  :type :ok, :f :add, :insert-value 1}
                           {:value 1, :time 3, :process 2,  :type :invoke, :f :upsert, :insert-value 2}
                           {:value 1, :time 4, :process 2,  :type :ok, :f :upsert, :insert-value 2}
                           {:value 2, :time 5, :process 3,  :type :invoke, :f :add, :insert-value 2}
                           {:value 2, :time 6, :process 3,  :type :ok, :f :add, :insert-value 2}
                           {:value 2, :time 7, :process 1,  :type :invoke, :f :upsert, :insert-value 3}
                           {:value 2, :time 8, :process 1,  :type :ok, :f :upsert, :insert-value 3}
                           {:value 3, :time 9, :process 2,  :type :invoke, :f :add, :insert-value 3}
                           {:value 3, :time 10, :process 2,  :type :ok, :f :add, :insert-value 3}
                           {:value 3, :time 11, :process 3,  :type :invoke, :f :upsert, :insert-value 4}
                           {:value 3, :time 12, :process 3,  :type :ok, :f :upsert, :insert-value 4}
                           {:value nil, :time 13, :process 1,  :type :invoke, :f :read}
                           {:value [[1 2] [2 3]], :time 14, :process 1,  :type :ok, :f :read}]
                          ;;If key 3 is not read even though it was both added and upserted
                          {})
           {:valid?              false
            :add-attempt-count     3
            :upsert-attempt-count 3
            :add-acknowledged-count  3
            :upsert-acknowledged-count  3
            :ok-count            2
            :lost-count          1
            :recovered-count     0
            :unexpected-count    0
            :upsert-not-rolled-back-count  0
            :add-not-rolled-back-count     0
            :ok                 "#{1..2}"
            :lost                "#{3}"
            :unexpected           "#{}"
            :upsert-not-rolled-back   "#{}"
            :add-not-rolled-back  "#{}"
            :upsert-rolled-back    "#{}"
            :add-rolled-back   "#{}"
            :recovered            "#{}"})))

  (testing "unexpected-reads"
    (is (= (checker/check (set-upsert-checker) nil
                          [{:value 1, :time 1, :process 1,  :type :invoke, :f :add, :insert-value 1}
                           {:value 1, :time 2, :process 1,  :type :ok, :f :add, :insert-value 1}
                           {:value 1, :time 3, :process 2,  :type :invoke, :f :upsert, :insert-value 2}
                           {:value 1, :time 4, :process 2,  :type :ok, :f :upsert, :insert-value 2}
                           {:value nil, :time 5, :process 3,  :type :invoke, :f :read}
                           {:value [[1 2] [2 3]], :time 6, :process 3,  :type :ok, :f :read}]
                          ;;If key 2 was read even though it was not added or upserted
                          {})
           {:valid?              false
            :add-attempt-count     1
            :upsert-attempt-count 1
            :add-acknowledged-count  1
            :upsert-acknowledged-count  1
            :ok-count            1
            :lost-count          0
            :recovered-count     0
            :unexpected-count    1
            :upsert-not-rolled-back-count  0
            :add-not-rolled-back-count     0
            :ok                 "#{1}"
            :lost                "#{}"
            :unexpected           "#{2}"
            :upsert-not-rolled-back   "#{}"
            :add-not-rolled-back  "#{}"
            :upsert-rolled-back    "#{}"
            :add-rolled-back   "#{}"
            :recovered            "#{}"}))))

(deftest set-delete-checker-test
  (testing "all correct"
    (is (= (checker/check (extended-set-checker) nil
                          [(invoke-op 1 :add 0)
                           (ok-op     1 :add 0)
                           (invoke-op 2 :add 1)
                           (ok-op     2 :add 1)
                           (invoke-op 1 :del 0)
                           (ok-op     1 :del 0)
                           (invoke-op 2 :read nil)
                           (ok-op     2 :read [1])]
                          {})
           (array-map
            :valid?              true
            :add-count           2
            :ok-count            2
            :lost-count          0
            :unexpected-count    0
            :indeterminate-count 0
            :recovered-count     0
            :not-recovered-count 0
            :ok                  "#{0..1}"
            :lost                "#{}"
            :unexpected          "#{}"
            :recovered           "#{}"
            :not-recovered       "#{}"))))

  (testing "lost"
    (is (= (checker/check (extended-set-checker) nil
                          [(invoke-op 1 :add 0)
                           (ok-op     1 :add 0)
                           (invoke-op 2 :add 1)
                           (ok-op     2 :add 1)
                           (invoke-op 1 :del 0)
                           (ok-op     1 :del 0)
                           (invoke-op 3 :add 2)
                           (ok-op     3 :add 2)
                           (invoke-op 2 :read nil)
                           (ok-op     2 :read [1])]
                          ;; set 2 was added and not deleted, but wasn't read.
                          {})
           (array-map
            :valid?              false
            :add-count           3
            :ok-count            2
            :lost-count          1
            :unexpected-count    0
            :indeterminate-count 0
            :recovered-count     0
            :not-recovered-count 0
            :ok                  "#{0..1}"
            :lost                "#{2}"
            :unexpected          "#{}"
            :recovered           "#{}"
            :not-recovered       "#{}"))))

  (testing "unexpected-reads"
    (is (= (checker/check (extended-set-checker) nil
                          [(invoke-op 1 :add 0)
                           (ok-op     1 :add 0)
                           (invoke-op 2 :add 1)
                           (ok-op     2 :add 1)
                           (invoke-op 1 :del 0)
                           (ok-op     1 :del 0)
                           (invoke-op 3 :add 2)
                           (ok-op     3 :add 2)
                           (invoke-op 2 :read nil)
                           (ok-op     2 :read [0 1 2])]
                          ;; set 0 was deleted, but was present in the read.
                          {})
           (array-map
            :valid?              false
            :add-count           3
            :ok-count            2
            :lost-count          0
            :unexpected-count    1
            :indeterminate-count 0
            :recovered-count     0
            :not-recovered-count 0
            :ok                  "#{1..2}"
            :lost                "#{}"
            :unexpected          "#{0}"
            :recovered           "#{}"
            :not-recovered       "#{}"))))

  (testing "indeterminate"
    (is (= (checker/check (extended-set-checker) nil
                          [(invoke-op 1 :del 0)
                           ;;deleting before adding  - Indeterminate delete
                           {:type :info, :f :del, :value 0, :msg "Set doesn't exist"}
                           ;;Not the exact 'msg' we see in the history. But
                           ;;we use this short msg for better readability here.
                           (invoke-op 2 :add 0)
                           (ok-op     2 :add 0)
                           (invoke-op 3 :add 1)
                           (ok-op     3 :add 1)
                           (invoke-op 3 :add 2)
                           (ok-op     3 :add 2)
                           (invoke-op 2 :read nil)
                           (ok-op     2 :read [0 1 2])]
                          {})
           (array-map
            :valid?              true
            :add-count           3
            :ok-count            3
            :lost-count          0
            :unexpected-count    0
            :indeterminate-count 1
            :recovered-count     1
            :not-recovered-count 0
            :ok                  "#{0..2}"
            :lost                "#{}"
            :unexpected          "#{}"
            :recovered           "#{0}"
            :not-recovered       "#{}"))))

  (testing "Failed ops"
    (is (= (checker/check (extended-set-checker) nil
                          [(invoke-op 2 :add 0)
                           (ok-op     2 :add 0)
                           (invoke-op 1 :del 0)
                           {:type :fail, :f :del, :value 0, :msg "TemporaryFailureException"}
                           ;;deleting set 0 failed, say maybe due to TemporaryFailure
                           (invoke-op 3 :add 1)
                           (ok-op     3 :add 1)
                           (invoke-op 3 :add 2)
                           {:type :fail, :f :add, :value 2, :msg "ServerOutOfMemoryException"}
                           ;;adding set 2 failed say maybe due to ServerOutOFMemoryException
                           (invoke-op 2 :read nil)
                           (ok-op     2 :read [0 1])]
                          {})
           (array-map
            :valid?              true
            :add-count           3
            :ok-count            3
            :lost-count          0
            :unexpected-count    0
            :indeterminate-count 0
            :recovered-count     0
            :not-recovered-count 0
            :ok                  "#{0..2}"
            :lost                "#{}"
            :unexpected          "#{}"
            :recovered           "#{}"
            :not-recovered       "#{}")))))
