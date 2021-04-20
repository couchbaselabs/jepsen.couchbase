(ns couchbase.checker_test
  (:refer-clojure :exclude [set])
  (:use clojure.test)
  (:require [jepsen.checker :as checker]
            [couchbase.checker :refer :all]
            [knossos
             [core :refer [ok-op invoke-op fail-op]]]))

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
