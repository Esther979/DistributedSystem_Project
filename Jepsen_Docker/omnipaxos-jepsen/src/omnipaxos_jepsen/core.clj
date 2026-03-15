(ns omnipaxos-jepsen.core
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.cli         :as cli]
            [jepsen.checker     :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.generator   :as gen]
            [jepsen.tests       :as tests]
            [knossos.model      :as model]
            [omnipaxos-jepsen.client  :as c]
            [omnipaxos-jepsen.nemesis :as n]))

(defn r   [_ _] {:type :invoke, :f :read,  :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 10)})
(defn cas [_ _] {:type :invoke, :f :cas,   :value [(rand-int 5) (rand-int 5)]})

(def standard-checker
  (checker/compose
    {:linearizable (checker/linearizable
                     {:model     (model/cas-register 0)
                      :algorithm :wgl})
     :timeline     (timeline/html)
     :stats        (checker/stats)
     :perf         (checker/perf)}))

;; ── 基本测试：网络分区 ─────────────────────────────────────────────────────────
(defn partition-test [opts]
  (merge tests/noop-test
         opts
         {:name      "omnipaxos-partition"
          :pure-generators true
          :nodes     ["localhost"]
          :client    (c/->OmniPaxosClient nil)
          :nemesis   (n/partition-nemesis)
          :generator (gen/phases
                       (gen/clients
                         (gen/once {:type :invoke, :f :write, :value 0}))
                       (gen/clients
                         (->> (gen/mix [r w cas])
                              (gen/stagger 0.5)
                              (gen/time-limit 5)))
                       (gen/mix [(gen/nemesis (gen/once {:type :info :f :start}))
                                 (gen/clients
                                   (->> (gen/mix [r w cas])
                                        (gen/stagger 0.5)
                                        (gen/time-limit 10)))])
                       (gen/nemesis (gen/once {:type :info :f :stop}))
                       (gen/clients
                         (->> (gen/mix [r w cas])
                              (gen/stagger 0.5)
                              (gen/time-limit 5))))
          :checker standard-checker}))

;; ── Bonus 测试：崩溃恢复 ───────────────────────────────────────────────────────
(defn crash-recovery-test [opts]
  (merge tests/noop-test
         opts
         {:name      "omnipaxos-crash-recovery"
          :pure-generators true
          :nodes     ["localhost"]
          :client    (c/->OmniPaxosClient nil)
          :nemesis   (n/crash-nemesis)
          :generator (gen/phases
                       (gen/clients
                         (gen/once {:type :invoke, :f :write, :value 0}))
                       (gen/clients
                         (->> (gen/mix [r w cas])
                              (gen/stagger 0.5)
                              (gen/time-limit 8)))
                       (gen/mix [(gen/nemesis (gen/once {:type :info :f :kill-leader}))
                                 (gen/clients
                                   (->> (gen/mix [r w cas])
                                        (gen/stagger 0.5)
                                        (gen/time-limit 8)))])
                       (gen/nemesis (gen/once {:type :info :f :restart}))
                       (gen/clients
                         (->> (gen/mix [r w cas])
                              (gen/stagger 0.5)
                              (gen/time-limit 10))))
          :checker standard-checker}))

;; ── CLI 入口 ──────────────────────────────────────────────────────────────────
(def cli-opts
  [[nil "--test-type TYPE" "Test type: partition or crash-recovery"
    :default "partition"]])

(defn -main [& args]
  (cli/run!
    (cli/single-test-cmd
      {:test-fn (fn [opts]
                  (case (:test-type opts)
                    "crash-recovery" (crash-recovery-test opts)
                    (partition-test opts)))
       :opt-spec cli-opts})
    args))
