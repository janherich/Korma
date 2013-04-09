(ns korma.core
  "Core querying and entity functions"
  (:require [clojure.string :as string]
            [korma.sql.engine :as eng]
            [korma.sql.fns :as sfns]
            [korma.sql.utils :as utils]
            [clojure.set :as set]
            [korma.db :as db])
  (:use [korma.sql.engine :only [bind-query bind-params]]))

(def ^{:dynamic true} *exec-mode* false)
(declare get-rel)
(declare add-deletion-of-relations)

;;*****************************************************
;; Query types
;;*****************************************************

(defn- check-ent [ent]
  (when-not (or (string? ent)
                (map? ent))
    (throw (Exception. (str "Invalid entity provided for the query: " ent)))))

(defn empty-query [ent]
  (let [ent (if (keyword? ent)
              (name ent)
              ent)
        [ent table alias db opts] (if (string? ent)
                                    [{:table ent} ent nil nil nil]
                                    [ent (:table ent) (:alias ent) 
                                     (:db ent) (get-in ent [:db :options])])]
    {:ent ent
     :table table
     :db db
     :options opts
     :alias alias}))

(defn select* 
  "Create an empty select query. Ent can either be an entity defined by defentity,
  or a string of the table name"
  [ent]
  (if (:type ent)
    ent
    (let [q (empty-query ent)
          fields (if (or (not (map? ent)) (empty? (:fields ent)))
                   [::*]
                   (conj (:fields ent) (:pk ent)))]
      (merge q {:type :select
                :fields fields
                :from [(:ent q)]
                :modifiers []
                :joins []
                :where []
                :order []
                :aliases #{}
                :group []
                :results :results}))))

(defn update* 
  "Create an empty update query. Ent can either be an entity defined by defentity,
  or a string of the table name."
  [ent]
  (if (:type ent)
    ent
    (let [q (empty-query ent)]
      (merge q {:type :update
                :fields {}
                :where []
                :results :keys}))))

(defn delete* 
  "Create an empty delete query. Ent can either be an entity defined by defentity,
  or a string of the table name"
  [ent]
  (if (:type ent)
    ent
    (let [q (empty-query ent)]
      (merge q {:type :delete
                :where []
                :results :keys}))))

(defn insert* 
  "Create an empty insert query. Ent can either be an entity defined by defentity,
  or a string of the table name"
  [ent]
  (if (:type ent)
    ent
    (let [q (empty-query ent)]
      (merge q {:type :insert
                :values []
                :results :keys}))))

;;*****************************************************
;; Query macros
;;*****************************************************

(defmacro select 
  "Creates a select query, applies any modifying functions in the body and then
  executes it. `ent` is either a string or an entity created by defentity.
  
  ex: (select user 
        (fields :name :email)
        (where {:id 2}))"
  [ent & body]
  `(let [query# (-> (select* ~ent)
                 ~@body)]
     (exec query#)))

(defmacro update 
  "Creates an update query, applies any modifying functions in the body and then
  executes it. `ent` is either a string or an entity created by defentity.
  
  ex: (update user 
        (set-fields {:name \"chris\"}) 
        (where {:id 4}))"
  [ent & body]
  `(let [query# (-> (update* ~ent)
                  ~@body)]
     (exec query#)))

(defmacro delete 
  "Creates a delete query, applies any modifying functions in the body and then
  executes it. `ent` is either a string or an entity created by defentity.
  
  ex: (delete user 
        (where {:id 7}))"
  [ent & body]
  `(let [query# (-> (delete* ~ent)
                  ~@body)]
     (exec query#)))

(defmacro insert 
  "Creates an insert query, applies any modifying functions in the body and then
  executes it. `ent` is either a string or an entity created by defentity. Inserts
  return the last inserted id.
  
  ex: (insert user 
        (values [{:name \"chris\"} {:name \"john\"}]))"
  [ent & body]
  `(let [query# (-> (insert* ~ent)
                  ~@body)]
     (exec query#)))

;;*****************************************************
;; Query parts
;;*****************************************************

(defn- add-aliases [query as]
  (update-in query [:aliases] set/union as))

(defn- update-fields [query fs]
  (let [[first-cur] (:fields query)]
    (if (= first-cur ::*)
      (assoc query :fields fs)
      (update-in query [:fields] concat fs))))

(defn all-fields
  "Select all fields from entity, useful also fro aggregate selects such as count"
  [query]
  (assoc query :fields [::*]))

(defn fields
  "Set the fields to be selected in a query. Fields can either be a keyword
  or a vector of two keywords [field alias]:
  
  (fields query :name [:firstname :first])"
  [query & vs] 
  (let [aliases (set (map second (filter coll? vs)))]
    (-> query
        (add-aliases aliases)
        (update-fields vs))))

(defn set-fields
  "Set the fields and values for an update query."
  [query fields-map]
  (update-in query [:set-fields] merge fields-map))

(defn from
  "Add tables to the from clause."
  [query table]
  (update-in query [:from] conj table))

(defn where*
  "Add a where clause to the query. Clause can be either a map or a string, and
  will be AND'ed to the other clauses."
  [query clause]
  (update-in query [:where] conj clause))

(defmacro where
  "Add a where clause to the query, expressing the clause in clojure expressions
  with keywords used to reference fields.
  e.g. (where query (or (= :hits 1) (> :hits 5)))

  Available predicates: and, or, =, not=, <, >, <=, >=, in, like, not

  Where can also take a map at any point and will create a clause that compares keys
  to values. The value can be a vector with one of the above predicate functions 
  describing how the key is related to the value: (where query {:name [like \"chris\"})"
  [query form]
  `(let [q# ~query]
     (where* q# ~(eng/parse-where `~form))))

(defn order
  "Add an ORDER BY clause to a select query. field should be a keyword of the field name, dir
  is ASC by default.
  
  (order query :created :asc)"
  [query field & [dir]]
  (update-in query [:order] conj [field (or dir :ASC)]))

(defn values
  "Add records to an insert clause. values can either be a vector of maps or a single
  map.
  
  (values query [{:name \"john\"} {:name \"ed\"}])"
  [query values]
  (update-in query [:values] concat (if (map? values)
                                      [values]
                                      values)))

(defn join* [query type table clause]
  (update-in query [:joins] conj [type table clause]))

(defmacro join 
  "Add a join clause to a select query, specifying the table name to join and the predicate
  to join on.
  
  (join query addresses (= :addres.users_id :users.id))
  (join query :right addresses (= :address.users_id :users.id))"
  ([query table clause]
   `(join* ~query :left ~table (eng/pred-map ~(eng/parse-where clause))))
  ([query type table clause]
   `(join* ~query ~type ~table (eng/pred-map ~(eng/parse-where clause)))))

(defn post-query
  "Add a function representing a query that should be executed for each result in a select.
  This is done lazily over the result set."
  [query post]
  (update-in query [:post-queries] conj post))

(defn limit
  "Add a limit clause to a select query."
  [query v]
  (assoc query :limit v))

(defn offset
  "Add an offset clause to a select query."
  [query v]
  (assoc query :offset v))

(defn group
  "Add a group-by clause to a select query"
  [query & fields]
  (update-in query [:group] concat fields))

(defmacro aggregate
  "Use a SQL aggregator function, aliasing the results, and optionally grouping by
  a field:
  
  (select users 
    (aggregate (count :*) :cnt :status))
  
  Aggregates available: count, sum, avg, min, max, first, last"
  [query agg alias & [group-by]]
  `(let [q# ~query]
     (bind-query q#
               (let [res# (fields q# [~(eng/parse-aggregate agg) ~alias])]
                 (if ~group-by
                   (group res# ~group-by)
                   res#)))))

;;*****************************************************
;; Other sql
;;*****************************************************

(defn sqlfn*
  "Call an arbitrary SQL function by providing the name of the function
  and its params"
  [fn-name & params]
  (apply eng/sql-func (name fn-name) params))

(defmacro sqlfn
  "Call an arbitrary SQL function by providing func as a symbol or keyword
  and its params"
  [func & params]
  `(sqlfn* (quote ~func) ~@params))

(defmacro subselect
  "Create a subselect clause to be used in queries. This works exactly like (select ...)
  execept it will wrap the query in ( .. ) and make sure it can be used in any current
  query:

  (select users
    (where {:id [in (subselect users2 (fields :id))]}))"
  [& parts]
  `(utils/sub-query (query-only (select ~@parts))))

(defn modifier
  "Add a modifer to the beginning of a query:

  (select orders
    (modifier \"DISTINCT\"))"
  [query & modifiers]
  (update-in query [:modifiers] conj (reduce str modifiers)))

(defn raw
  "Embed a raw string of SQL in a query. This is used when Korma doesn't
  provide some specific functionality you're looking for:

  (select users
    (fields (raw \"PERIOD(NOW(), NOW())\")))"
  [s]
  (utils/generated s))

;;*****************************************************
;; Query exec
;;*****************************************************

(defmacro sql-only
  "Wrap around a set of queries so that instead of executing, each will return a string of the SQL 
  that would be used."
  [& body]
  `(binding [*exec-mode* :sql]
     ~@body))

(defmacro dry-run
  "Wrap around a set of queries to print to the console all SQL that would 
  be run and return dummy values instead of executing them."
  [& body]
  `(binding [*exec-mode* :dry-run]
     ~@body))

(defmacro query-only
  "Wrap around a set of queries to force them to return their query objects."
  [& body]
  `(binding [*exec-mode* :query]
     ~@body))

(defn as-sql
  "Force a query to return a string of SQL when (exec) is called."
  [query]
  (bind-query query (:sql-str (eng/->sql query))))

(defn- apply-posts
  [query results]
  (if-let [posts (seq (:post-queries query))]
    (let [post-fn (apply comp posts)]
      (post-fn results))
    results))

(defn- apply-transforms
  [query results]
  (if (not= (:type query) :select)
    results
    (if-let [trans (seq (-> query :ent :transforms))]
      (let [trans-fn (apply comp trans)]
        (map trans-fn results))
      results)))

(defn- apply-prepares
  [query]
  (if-let [preps (seq (-> query :ent :prepares))]
    (let [preps (apply comp preps)]
      (condp = (:type query)
        :insert (let [values (:values query)]
                  (assoc query :values (map preps values)))
        :update (let [value (:set-fields query)]
                  (assoc query :set-fields (preps value)))
        query))
    query))

(defn transform-where
  [query]
  (update-in query [:where] (fn [clauses] (map (fn [clause] (eng/pred-map clause)) clauses))))

(defn exec
  "Execute a query map and return the results."
  [query]
  (let [query (apply-prepares query)
        query (transform-where query)
        query (bind-query query (eng/->sql query))
        sql (:sql-str query)
        params (:params query)]
    (cond
      (:sql query) sql
      (= *exec-mode* :sql) sql
      (= *exec-mode* :query) query
      (= *exec-mode* :dry-run) (do
                                 (println "dry run ::" sql "::" (vec params))
                                 (let [pk (-> query :ent :pk)
                                       results (apply-posts query [{pk 1}])]
                                   (first results)
                                   results))
      :else (let [results (db/do-query query)]
              (apply-transforms query (apply-posts query results))))))

(defn exec-raw
  "Execute a raw SQL string, supplying whether results should be returned. `sql` can either be
  a string or a vector of the sql string and its params. You can also optionally
  provide the connection to execute against as the first parameter.

  (exec-raw [\"SELECT * FROM users WHERE age > ?\" [5]] :results)"
  [conn? & [sql with-results?]]
  (let [sql-vec (fn [v] (if (vector? v) v [v nil]))
        [conn? [sql-str params] with-results?] (if (or (string? conn?)
                                                       (vector? conn?))
                                                 [nil (sql-vec conn?) sql]
                                                 [conn? (sql-vec sql) with-results?])]
    (db/do-query {:db conn? :results with-results? :sql-str sql-str :params params})))

;;*****************************************************
;; Entities
;;*****************************************************

(defn create-entity
  "Create an entity representing a table in a database."
  [table]
  {:table table
   :name table
   :pk :id
   :db nil
   :transforms '()
   :prepares '()
   :fields []
   :rel {}})

(defn entity-fields
  "Set the fields to be retrieved by default in select queries for the
  entity."
  [ent & fields]
  (update-in ent [:fields] concat fields))
             ;(map #(eng/prefix ent %) 
             ;     fields)))

(defn table
  "Set the name of the table and an optional alias to be used for the entity. 
  By default the table is the name of entity's symbol."
  [ent t & [alias]]
  (let [tname (if (or (keyword? t)
                      (string? t))
                (name t)
                (if alias
                  t
                  (throw (Exception. "Generated tables must have aliases."))))
        ent (assoc ent :table tname)]
    (if alias
      (assoc ent :alias (name alias))
      ent)))

(defn pk
  "Set the primary key used for an entity. :id by default."
  [ent pk]
  (assoc ent :pk (keyword pk)))

(defn database
  "Set the database connection to be used for this entity."
  [ent db]
  (assoc ent :db db))

(defn transform
  "Add a function to be applied to results coming from the database"
  [ent func]
  (update-in ent [:transforms] conj func))

(defn prepare
  "Add a function to be applied to records/values going into the database"
  [ent func]
  (update-in ent [:prepares] conj func))

(defmacro defentity
  "Define an entity representing a table in the database, applying any modifications in
  the body."
  [ent & body]
  `(let [e# (-> (create-entity ~(name ent))
              ~@body)]
     (def ~ent e#)))

;;*****************************************************
;; Relation switch
;;*****************************************************

(defn relation-switch 
  "Implements switch for relation types, takes function to be called for each relation type
   and variable number of arguments after. Each function will be called with relation query,
   sub-entity and optional other arguments if those are provided."
  [query sub-ent rel-type has-one-f has-many-f has-many-to-many-f belongs-to-many-to-many-f & args]
    (condp = rel-type
      :has-one (apply has-one-f query sub-ent args) 
      :has-many (apply has-many-f query sub-ent args)
      :has-many-to-many (apply has-many-to-many-f query sub-ent args)
      :belongs-to-many-to-many (apply belongs-to-many-to-many-f query sub-ent args)
      (throw (Exception. (str "Unknown type of relaton: " rel-type " defined for table: " (:table sub-ent))))))

;;*****************************************************
;; Create relations
;;*****************************************************

(defn create-many-to-many-relation
  "Create a relation map for many-to-many relations"
  [ent sub-ent type opts]
  (let [d-fk (keyword (str (:table ent) "_id"))
      d-sub-fk (keyword (str (:table sub-ent) "_id"))
      d-map-table (condp = type
                :has-many-to-many (str (:table ent) "2" (:table sub-ent))
                :belongs-to-many-to-many (str (:table sub-ent) "2" (:table ent)))
      opts (merge {:fk d-fk
                   :sub-fk d-sub-fk
                   :map-table d-map-table}
                  opts)
      [pk fk sub-pk sub-fk] [(:pk ent)
                             (:fk opts)
                             (:pk ent)
                             (:sub-fk opts)]]
            {:table (:table sub-ent)
             :alias (:alias sub-ent)
             :rel-type type
             :pk pk
             :sub-pk sub-pk
             :fk fk
             :sub-fk sub-fk
             :map-table (:map-table opts)}))

(defn create-has-many-relation
  "Create a relation map for has-one or has-many relation"
  [ent sub-ent type opts]
  (let [d-fk (keyword (str (:table ent) "_id"))
        opts (merge {:fk d-fk} opts)
       [pk fk] [(:pk ent)
                (:fk opts)]]
			  {:table (:table sub-ent)
			   :alias (:alias sub-ent)
			   :rel-type type
			   :pk pk
			   :fk fk}))

(defn create-has-one-relation
  "Create a relation map for belongs-to relation"
  [ent sub-ent type opts]
  (let [d-fk (keyword (str (:table sub-ent) "_id"))
      opts (merge {:fk d-fk} opts)
      [pk fk] [(:pk sub-ent)
               (:fk opts)]]
		   {:table (:table sub-ent)
		    :alias (:alias sub-ent)
		    :rel-type type
		    :pk pk
		    :fk fk}))

(defn rel
  [ent sub-ent type opts]
  (let [var-name (-> sub-ent meta :name)
        cur-ns *ns*]
    (assoc-in ent [:rel (keyword (name var-name))]
              (delay
                (let [resolved (ns-resolve cur-ns var-name)
                      sub-ent (when resolved
                                (deref sub-ent))]
                  (when-not (map? sub-ent)
                    (throw (Exception. (format "Entity used in relationship does not exist: %s" (name var-name)))))
                  (-> (relation-switch ent sub-ent type create-has-one-relation
                                                        create-has-many-relation
                                                        create-many-to-many-relation
                                                        create-many-to-many-relation type opts) 
                      (assoc :sub-ent sub-ent)))))))

(defn get-rel [ent sub-ent]
  (let [sub-name (cond
                   (map? sub-ent) (keyword (:name sub-ent))
                   (keyword? sub-ent) sub-ent
                   :else (keyword sub-ent))]
    (force (get-in ent [:rel sub-name]))))

(defmacro has-one
  "Add a belongs-to relationship for the given entity. It is assumed that the foreign key
  is on the current entity with the format sub-ent-table_id: email.user_id = user.id.
  Opts can include a key for :fk to explicitly set the foreign key.

  (has-one users email {:fk :emailID})"
  [ent sub-ent & [opts]]
  `(rel ~ent (var ~sub-ent) :has-one ~opts))

(defmacro has-many
  "Add a has-many relation for the given entity. It is assumed that the foreign key
  is on the sub-entity with the format table_id: user.id = email.user_id
  Opts can include a key for :fk to explicitly set the foreign key.
  
  (has-many users email {:fk :emailID})"
  [ent sub-ent & [opts]]
  `(rel ~ent (var ~sub-ent) :has-many ~opts))

(defmacro has-many-to-many
  "Adds a many-many relation for given entity. It is assumed that mapping table
  exists between entity and sub-entity with name entity2sub-entity: user2email
  and this table has columns table_id and sub-ent-table_id: user_id, email_id.
  Opts can include a key for :fk to explicitly set the entity foreign key in 
  mapping table, a key :sub-fk to explicitly set the sub-entity foreign key in mapping
  table and finally also a key :map-table to explicity set the name of mapping table.

  (has-many-many users email {:fk :userID :sub-fk :emailID :map-table \"users2emails\"})"
  [ent sub-ent & [opts]]
  `(rel ~ent (var ~sub-ent) :has-many-to-many ~opts))

(defmacro belongs-to-many-to-many
  "Adds a many-many relation for given entity. It is assumed that mapping table
  exists between entity and sub-entity with name sub-entity2entity: email2user
  and this table has columns table_id and sub-ent-table_id: user_id, email_id.
  Opts can include a key for :fk to explicitly set the entity foreign key in 
  mapping table, a key :sub-fk to explicitly set the sub-entity foreign key in mapping
  table and finally also a key :map-table to explicity set the name of mapping table.

  (has-many-many users email {:fk :userID :sub-fk :emailID :map-table \"emails2users\"})"
  [ent sub-ent & [opts]]
  `(rel ~ent (var ~sub-ent) :belongs-to-many-to-many ~opts))

;;*****************************************************
;; Relation functions
;;*****************************************************

(defn- select-has-one [query sub-ent rel func]
  (let [fk (:fk rel)
        pk (raw (eng/prefix sub-ent (:pk rel)))
        table (keyword (eng/table-alias sub-ent))]
    (post-query (fields query fk) 
        (partial map 
                 #(dissoc (assoc % table
                                 (first (select sub-ent
                                                (func)
                                                (where {pk (get % fk)})))) fk)))))

(defn- select-has-many [query sub-ent rel func]
  (let [pk (:pk rel)
        fk (keyword (eng/prefix sub-ent (:fk rel)))
        table (keyword (eng/table-alias sub-ent))]
    (post-query query 
        (partial map 
                 #(assoc % table
                         (select sub-ent
                                 (func)
                                 (where {fk (get % pk)})))))))

(defn- select-many-to-many [query sub-ent rel func]
  (let [ent (:ent query)
        map-table (:map-table rel)
        pk (:pk rel)
        fk (raw (eng/prefix map-table (:fk rel)))
        sub-pk (raw (eng/prefix sub-ent (:sub-pk rel)))
        sub-fk (raw (eng/prefix map-table (:sub-fk rel)))
        table (keyword (eng/table-alias sub-ent))]
    (post-query query 
        (partial map 
                 #(assoc % table
                         (select sub-ent
                                 (join :inner map-table (= sub-pk sub-fk))
                                 (func)
                                 (where {fk (get % pk)})))))))

(defn- select-relations-has-one [query sub-ent rel]
  (let [fk (:fk rel)
        table (keyword (eng/table-alias sub-ent))]
    (post-query (fields query fk) 
        (partial map 
                 #(dissoc (assoc % table (get % fk)) fk)))))

(defn- select-relations-has-many [query sub-ent rel]
  (let [pk (:pk rel)
        pk-sub (:pk sub-ent)
        fk (keyword (eng/prefix sub-ent (:fk rel)))
        table (keyword (eng/table-alias sub-ent))]
    (post-query query 
        (partial map 
                 #(assoc % table
                         (map pk-sub (select sub-ent
                                             (where {fk (get % pk)}))))))))

(defn- select-relations-many-to-many [query sub-ent rel]
  (let [map-table (:map-table rel)
        pk (:pk rel)
        fk (raw (eng/prefix map-table (:fk rel)))
        sub-fk (:sub-fk rel)
        table (keyword (eng/table-alias sub-ent))]
    (post-query query 
        (partial map 
                 #(assoc % table
                         (map sub-fk (select map-table
                                             (where {fk (get % pk)}))))))))

(defn- extract-id [result]
  (first (vals result)))

(defn- insert-update-one [key query sub-ent rel]
  (let [fk (:fk rel)
        relations (get-in query [:relations (:table sub-ent)])]
    (if (coll? relations)
      (throw (Exception. (str "There must be only single value defined for relation of type "
                              (:rel-type rel) " on related entity " (:table sub-ent))))
      (update-in query [key] (fn [values]
                                   (if (map? values)
                                     (assoc values fk relations)
                                     (map #(assoc % fk relations) values)))))))

(defn- insert-many [query sub-ent rel]
  (let [fk (:fk rel)
        pk (:pk rel)
        table (keyword (eng/table-alias sub-ent))
        relations (get-in query [:relations (:table sub-ent)])]
    (if (coll? relations)
      (let [rel-vector (into [] relations)]
	      (post-query query
	                  (fn [result]
	                    (let [id (extract-id result)]
	                      (update sub-ent
	                            (set-fields {fk id})
	                            (where {pk [in rel-vector]}))
	                      id))))
      (throw (Exception. (str "There must be a collection of values defined for relation of type "
                              (:rel-type rel) " on related entity " (:table sub-ent)))))))

(defn- insert-many-to-many [query sub-ent rel]
  (let [fk (:fk rel)
        sub-fk (:sub-fk rel)
        map-table (:map-table rel)
        relations (get-in query [:relations (:table sub-ent)])]
    (if (coll? relations)
      (post-query query
                  (fn [result] 
                    (let [id (extract-id result)
                          vals (into [] (map #(assoc (hash-map fk id) sub-fk %) relations))]
                      (when (seq vals)
	                      (insert map-table
	                            (values vals)))
                      id)))
      (throw (Exception. (str "There must be a collection of values defined for relation of type "
                              (:rel-type rel) " on related entity " (:table sub-ent)))))))

(defn- extract-id-from-where-condition [clauses id-key]
  (if-let [id (id-key (first (filter #(contains? % id-key) clauses)))]
    id
    (throw (Exception. (str "No key " id-key " found in where clauses " clauses)))))

(defn- update-many [query sub-ent rel]
  (let [fk (:fk rel)
        pk (:pk rel)
        table (keyword (eng/table-alias sub-ent))
        relations (get-in query [:relations (:table sub-ent)])
        id (extract-id-from-where-condition (:where query) pk)]
    (if (coll? relations)
      (let [rel-vector (into [] relations)]
	      (post-query query
	                  (fn [_]
	                    (update sub-ent
	                          (set-fields {fk nil})
	                          (where {fk id}))
	                    (when (seq rel-vector)
	                      (update sub-ent
	                            (set-fields {fk id})
	                            (where {pk ['in rel-vector]}))))))
      (throw (Exception. (str "There must be a collection of values defined for relation of type "
                              (:rel-type rel) " on related entity " (:table sub-ent)))))))

(defn- update-many-to-many [query sub-ent rel]
  (let [pk (:pk rel)
        fk (:fk rel)
        sub-fk (:sub-fk rel)
        map-table (:map-table rel)
        relations (get-in query [:relations (:table sub-ent)])
        id (extract-id-from-where-condition (:where query) pk)
        vals (into [] (map #(assoc (hash-map fk id) sub-fk %) relations))]
    (if (coll? relations)
      (post-query query
                  (fn [_]
                    (delete map-table
                          (where {fk id}))
                    (when (seq vals)
                      (insert map-table
                            (values vals)))))
      (throw (Exception. (str "There must be a collection of values defined for relation of type "
                              (:rel-type rel) " on related entity " (:table sub-ent)))))))

(defn- delete-many [query sub-ent rel]
  (let [pk (:pk rel)
        fk (:fk rel)
        table (keyword (eng/table-alias sub-ent))
        id (extract-id-from-where-condition (:where query) pk)]
    (post-query query
                (fn [_]
                  (update sub-ent
                        (set-fields {fk nil})
                        (where {fk id}))))))

(defn- delete-many-to-many [query sub-ent rel]
  (let [pk (:pk rel)
        fk (:fk rel)
        sub-fk (:sub-fk rel)
        map-table (:map-table rel)
        id (extract-id-from-where-condition (:where query) pk)] 
    (post-query query
                (fn [_]
                  (delete map-table
                        (where {fk id}))))))

(defn add-deletion-of-relations [q]
  "Add post functions for deletion of all relations for given entity"
  (loop [query q relations (vals (:rel (:ent query)))]
     (if (first relations)
       (let [rel (force (first relations))
             sub-ent (:sub-ent rel)]
         (recur (relation-switch query sub-ent (:rel-type rel) (fn [query _ _] query)
                                                               delete-many
                                                               delete-many-to-many
                                                               delete-many-to-many rel) (rest relations)))
       query)))

(defn relations
  "Add relations for insert and update clauses, relations must be map with vectors of primary keys 
   of related entities.

  (relations query {:roles [1 2 3]})

  Or in case of has-many and belongs-to relationships values of those map must be just single keys.

  (relations query {:users 1})"
  [query relation-map]
  (if (first relation-map)
    (let [item (first relation-map)
          rel (get-rel (:ent query) (key item))
          sub-ent (:sub-ent rel)
          relations (val item)
          q (assoc-in query [:relations (:table sub-ent)] relations)]
      (if (not rel)
        (throw (Exception. (str "No relationship defined for table: " (:table sub-ent))))
        (condp = (:type query)
          :insert (recur (relation-switch q sub-ent (:rel-type rel) (partial insert-update-one :values) 
                                                                    insert-many 
                                                                    insert-many-to-many
                                                                    insert-many-to-many rel) (rest relation-map))
          :update (recur (relation-switch q sub-ent (:rel-type rel) (partial insert-update-one :set-fields) 
                                                                    update-many 
                                                                    update-many-to-many
                                                                    update-many-to-many rel) (rest relation-map))
          (throw (Exception. (str "Function relations is not allowed for " (:type query) " type queries."))))))
    query))

(defn with-relations
  "Select relations as links to other entities.

  (with-relations roles)"
  [query sub-ent]
  (let [rel (get-rel (:ent query) sub-ent)]
    (if (not rel)
      (throw (Exception. (str "No relationship defined for table: " (:table sub-ent))))
      (relation-switch query sub-ent (:rel-type rel) select-relations-has-one 
                                                     select-relations-has-many 
                                                     select-relations-many-to-many
                                                     select-relations-many-to-many rel))))

;;*****************************************************
;; Where-relations
;;*****************************************************

(defn- where-one [query sub-ent rel]
  (let [fk (:fk rel)
        relations (get-in query [:where-relations (:table sub-ent)])]
    (where query {fk relations})))

(defn- where-many [query sub-ent rel]
  (let [ent (:ent query)
        pk (raw (eng/prefix ent (:pk rel)))
        fk (raw (eng/prefix sub-ent (:fk rel)))
        sub-ent-table (:table sub-ent)
        relations (get-in query [:where-relations (:table sub-ent)])
        query (join query :inner sub-ent-table (= pk fk))]
    (where query {fk relations})))

(defn- where-many-to-many [query sub-ent rel]
  (let [ent (:ent query)
        map-table (:map-table rel)
        pk (raw (eng/prefix ent (:pk rel)))
        fk (raw (eng/prefix map-table (:fk rel)))
        sub-pk (raw (eng/prefix sub-ent (:sub-pk rel)))
        sub-fk (raw (eng/prefix map-table (:sub-fk rel)))
        relations (get-in query [:where-relations (:table sub-ent)])
        query (join query :inner map-table (= pk fk))]
    (where query {sub-fk relations})))

(defn where-relations
  "Add where conditions for relations to query, relations should be map with either single keys or vectors
   representing entity identities, for example:

   (where-relations query {:projects 1 :users [1 2 3]})"
  [query relation-map]
  (if (first relation-map)
    (let [item (first relation-map)
          rel (get-rel (:ent query) (key item))
          sub-ent (:sub-ent rel)
          relations (val item)
          q (assoc-in query [:where-relations (:table sub-ent)] relations)]
      (if (not rel)
        (throw (Exception. (str "No relationship defined for table: " (:table sub-ent))))
        (recur (relation-switch q sub-ent (:rel-type rel) where-one 
                                                          where-many 
                                                          where-many-to-many
                                                          where-many-to-many rel) (rest relation-map))))
    query))

;;*****************************************************
;; With
;;*****************************************************

(defn with* [query sub-ent func]
  (let [rel (get-rel (:ent query) sub-ent)]
    (if (not rel)
      (throw (Exception. (str "No relationship defined for table: " (:table sub-ent))))
      (relation-switch query sub-ent (:rel-type rel) select-has-one 
                                                     select-has-many 
                                                     select-many-to-many
                                                     select-many-to-many rel func))))

(defmacro with
  "Add a related entity to the given select query. If the entity has a relationship
  type of :belongs-to or :has-one, the requested fields will be returned directly in
  the result map. If the entity is a :has-many, a second query will be executed lazily
  and a key of the entity name will be assoc'd with a vector of the results.

  (defentity email (entity-fields :email))
  (defentity user (has-many email))
  (select user
    (with email) => [{:name \"chris\" :email [{email: \"c@c.com\"}]} ...

  With can also take a body that will further refine the relation:
  (select user
     (with address
        (with state)
        (fields :address.city :state.state)
        (where {:address.zip x})))"
  [query ent & body]
  `(with* ~query ~ent (fn [q#]
                        (-> q#
                            ~@body))))

(defmacro count-entity
  "Counts given entity, with any korma expressions to restrict query"
  [ent & body]
  `(let [query# (-> (select* ~ent)
                  (all-fields)
	                (aggregate (~'count :*) :cnt)
                 ~@body)]
     (:cnt (first (exec query#)))))
