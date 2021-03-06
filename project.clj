(defproject korma-enhanced "0.3.1"
  :description "Tasty SQL for Clojure"
  :url "http://github.com/janherich/Korma"
  :dependencies [[org.clojure/clojure "1.5.0"]
                 [c3p0/c3p0 "0.9.1.2"]
                 [org.clojure/java.jdbc "0.2.3"]]
  :codox {:exclude [korma.sql.engine korma.sql.fns korma.sql.utils]}
          :dev-dependencies [[postgresql "9.0-801.jdbc4"]])
