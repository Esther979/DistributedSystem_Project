(ns omnipaxos-jepsen.nemesis
  (:require [jepsen.nemesis :as nemesis]
            [clojure.java.shell :as shell]
            [clojure.tools.logging :refer [info warn]]))

;; ── 网络分区 Nemesis（基本测试用）────────────────────────────────────────────
(defn partition-nemesis []
  (reify nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (case (:f op)
        :start
        (do
          (info "Nemesis: pausing s2 and s3 (isolating leader s1)")
          (shell/sh "docker" "pause" "s2")
          (shell/sh "docker" "pause" "s3")
          (assoc op :type :info :value "paused s2, s3"))

        :stop
        (do
          (info "Nemesis: unpausing s2 and s3")
          (shell/sh "docker" "unpause" "s2")
          (shell/sh "docker" "unpause" "s3")
          (assoc op :type :info :value "unpaused s2, s3"))

        (assoc op :type :info :value :ignored)))

    (teardown! [this test]
      (shell/sh "docker" "unpause" "s2")
      (shell/sh "docker" "unpause" "s3"))))

;; ── 崩溃恢复 Nemesis（Bonus 用）──────────────────────────────────────────────
;; 随机 kill 节点（包括 leader s1），然后重启，验证数据不丢失
(defn crash-nemesis []
  (reify nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (case (:f op)
        ;; kill：随机停止一个节点（包括 leader）
        :kill
        (let [node (rand-nth ["s1" "s2" "s3"])]
          (info "Nemesis: killing" node)
          (shell/sh "docker" "stop" node)
          (assoc op :type :info :value (str "killed " node)))

        ;; kill-leader：专门 kill leader s1（最危险的场景）
        :kill-leader
        (do
          (info "Nemesis: killing leader s1")
          (shell/sh "docker" "stop" "s1")
          (assoc op :type :info :value "killed s1"))

        ;; restart：重启所有停止的节点
        :restart
        (do
          (info "Nemesis: restarting all nodes")
          (shell/sh "docker" "start" "s1")
          (shell/sh "docker" "start" "s2")
          (shell/sh "docker" "start" "s3")
          ;; 等待节点重启并重新选举 leader
          (Thread/sleep 5000)
          (assoc op :type :info :value "restarted all nodes"))

        (assoc op :type :info :value :ignored)))

    (teardown! [this test]
      ;; 确保所有节点都在运行
      (shell/sh "docker" "start" "s1")
      (shell/sh "docker" "start" "s2")
      (shell/sh "docker" "start" "s3"))))
