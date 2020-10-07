(ns arctype.service.sqlite
  (:require
    [clojure.core.async :as async]
    [clojure.java.jdbc :as jdbc]
    [clojure.tools.logging :as log]
    [schema.core :as S]
    [sundbry.resource :as resource :refer [with-resources]]
    [arctype.service.protocol :refer :all])
  (:import
    [java.sql SQLException]))

(def Config
  {:db-spec {S/Keyword S/Any}})

(defrecord Connection [connection]
  
  java.io.Closeable
  (close [this]
    (log/debug {:message "Closing connection to SQLite"})
    (.close connection)
    nil))

(defn- connect
  "Open a jdbc connection"
  [{:keys [config]}]
  (let [db-spec (:db-spec config)]
    (log/debug {:message "Opening connection to SQLite"
                :db-spec db-spec})
    (->Connection (jdbc/get-connection db-spec))))

(defmacro try-sql
  "Macro to log inner sql exceptions for better debugging"
  [& body]
  `(try 
     (do ~@body)
     (catch SQLException e#
       (let [inner# (.getNextException e#)]
         (log/error e# {:message "SQL exception"
                        :cause (when (some? inner#)
                                 (.getMessage inner#))}))
       (throw e#))))

(defn with-locking-tx*
  [{:keys [connection-state] :as this} tx-fn]
  (locking (:lock this)
    (let [cxn (or @connection-state (reset! connection-state (connect this)))]
      (try 
        (jdbc/db-transaction* cxn tx-fn)
        (catch SQLException e
          (let [inner (.getNextException e)]
            (log/error e {:message "SQL exception"
                          :cause (when (some? inner) (.getMessage inner))}))
          ; disconnect on error
          (swap! connection-state #(.close %))
          (throw e))))))

(defmacro with-locking-tx
  [[tx this] & body]
  (let [fn-bindings [tx]]
    `(with-locking-tx* ~this (^{:once true} fn* ~fn-bindings ~@body))))

(defn health-check!
  [this]
  (with-locking-tx [tx this]
    (jdbc/query tx ["SELECT version()"])))

(defn first-val
  "Get the first column of the first row"
  [results]
  (second (ffirst results)))

(defrecord SQLiteClient [config connection-state lock]
  PLifecycle
  (start [this]
    (as-> this this
        (assoc this :lock (Object.))
        (assoc this :connection-state (atom nil))
        (with-locking-tx [tx this]
          (let [version (first-val (jdbc/query tx ["SELECT sqlite_version() as version"]))]
            (log/info "Opened SQLite database with version:" version))
          this)))

  (stop [this]
    (log/info "Stopping SQLite client")
    (-> this
        (dissoc :connection-state)
        (dissoc :lock)))

  PJdbcConnection
  (conn [this] (connect this)))

(S/defn create
  [resource-name config :- Config]
  (resource/make-resource
    (map->SQLiteClient
      {:config config})
    resource-name))
