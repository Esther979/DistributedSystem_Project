(ns omnipaxos-jepsen.client
  (:require [clj-http.client :as http]
            [cheshire.core   :as json]
            [jepsen.client   :as client]
            [clojure.tools.logging :refer [info warn]]))

(defn random-port []
  (rand-nth [8081 8082 8083]))

(defn post! [port path body]
  (try
    (let [resp (http/post (str "http://localhost:" port path)
                          {:content-type    :json
                           :body            (json/generate-string body)
                           :socket-timeout  5000
                           :conn-timeout    2000
                           :throw-exceptions false})]
      (json/parse-string (:body resp) true))
    (catch java.net.ConnectException _ nil)
    (catch java.net.SocketTimeoutException _ nil)
    (catch Exception _ nil)))

(defrecord OmniPaxosClient [conn]
  client/Client

  (open!    [this test node] this)
  (setup!   [this test] this)
  (teardown![this test])
  (close!   [this test] this)

  (invoke! [this test op]
    (let [port (random-port)]
      (case (:f op)

        :read
        (if-let [res (post! port "/get" {:key "k"})]
          (if (:ok res)
            (assoc op :type :ok
                      :value (when-let [v (:value res)]
                               (Long/parseLong v)))
            (assoc op :type :fail :error (:error res)))
          (assoc op :type :info :error :timeout))

        :write
        (if-let [res (post! port "/put"
                            {:key "k" :value (str (:value op))})]
          (if (:ok res)
            (assoc op :type :ok)
            (assoc op :type :fail :error (:error res)))
          (assoc op :type :info :error :timeout))

        :cas
        (let [[from to] (:value op)]
          (if-let [res (post! port "/cas"
                              {:key  "k"
                               :from (str from)
                               :to   (str to)})]
            (cond
              (:ok res)
              (assoc op :type :ok)

              (= "precondition-failed" (:error res))
              (assoc op :type :fail :error :precondition-failed)

              :else
              (assoc op :type :fail :error (:error res)))
            (assoc op :type :info :error :timeout)))

        ;; 兜底：client 不处理 nemesis 操作
        (assoc op :type :info :error :not-a-client-op)))))
