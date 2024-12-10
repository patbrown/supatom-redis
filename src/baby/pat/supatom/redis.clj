(ns baby.pat.supatom.redis
  (:require [orchestra.core :refer [defn-spec]]
            [baby.pat.secrets :as !]
            [baby.pat.supatom]
            [baby.pat.jes.vt :as vt]
            [taoensso.carmine :as redis]
            [taoensso.nippy :refer [freeze thaw]]))

(defn *get [config k]
  (redis/wcar config (redis/get k)))

(defn *set [config k v]
  (redis/wcar config (redis/set k v)))

(defn *delete [config k]
  (redis/wcar config (redis/del k)))

(defn *keys
  ([config] (*keys config "*"))
  ([config k]
   (redis/wcar config (redis/keys k))))

(defn *exists? [config k]
  (= 1 (redis/wcar config (redis/exists k))))

;; ### COMMIT!
(defn-spec commit! ::vt/discard
  "Commits contents to a redis key."
  ([connection ::vt/map id ::vt/qkw contents ::vt/any]
   (*set connection (str id) contents)))

;; ### SNAPSHOT
(defn-spec snapshot ::vt/discard
  "Returns a snapshot from a file."
  ([connection ::vt/map id ::vt/qkw]
   (let [id (str id)]
     (if (*exists? connection id)
       (*get connection id)
       (commit! connection id nil)))))

(defmethod baby.pat.supatom/commit-with :redis/default [{:keys [id write-with connection] :as this}]
  (commit! connection id (write-with @this)))

(defmethod baby.pat.supatom/snapshot-with :redis/default [{:keys [id read-with connection] :as this}]
  (read-with (snapshot connection id)))

(def supatom-redis-default-overlay {:variant :redis/default
                                    :write-with freeze
                                    :read-with thaw})

(comment
  (defn connect-with [variant]
    {:pool (redis/connection-pool {})
     :spec {:uri (!/get-secret (keyword (str (namespace variant) "." (name variant)) "connection-string"))
            :ssl-fn :default}})
  (def hello (supatom {:id :based/dude
                       :connection (connect-with :redis/default)}))
;
  )
