(defproject omnipaxos-jepsen "0.1.0"
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.5"]
                 [clj-http "3.12.3"]
                 [cheshire "5.12.0"]]
  :main omnipaxos-jepsen.core
  :jvm-opts ["-Djava.awt.headless=true"])
