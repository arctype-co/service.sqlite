(defproject arctype/service.sqlite "0.1.1"
  :dependencies 
  [[org.clojure/clojure "1.10.0"]
   [org.clojure/core.async "0.4.500"]
   [org.clojure/java.jdbc "0.7.11"]
   [arctype/service "1.1.0-SNAPSHOT"]
   [org.xerial/sqlite-jdbc "3.32.3.2"]]
  
  :profiles
  {:dev
   {:resource-paths ["test"]
    :dependencies
    [[org.apache.logging.log4j/log4j-core "2.13.2"]]}})
