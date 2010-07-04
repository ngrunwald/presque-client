(ns presque-client.client
  (:use [clojure.contrib.http.agent :exclude [bytes]]
        [clojure.contrib.condition])
  (:require [org.danlarkin.json :as json]))

(defn format-params
  [params]
  (if (> (count params) 0)
    (str "?" (apply str (map #(str "&" (name (first %)) "=" (second %)) (partition 2 params))))
    ""))
  

(defn connect-presque
  [& [url headers]]
  {:base-url (or url "http://localhost:5000/")
   :headers (or headers {"Content-Type" "application/json"})
   :connect-timeout 1000
   :read-timeout 5000})

(defn agent-request
  [path method conn & rest]
  (let [args (apply hash-map rest)
        url (str (:base-url conn)
                 path
                 (format-params (:params args)))]
    (http-agent url
                :method method
                :body (:body args)
                :headers (:headers conn)
                :connection-timeout (:connection-timeout conn)
                :read-timeout (:read-timeout conn))))

(defn check-return-code
  [agent expected fmt]
  (await-for 5000 agent)
  (let [ag-error (agent-error agent)]
    (if ag-error 
        (raise
         :type :connection-error
         :exception ag-error)))
  (let [code (status agent)
        msg (message agent)]
    (if-not (some #(= % code) expected)
      (raise
       :type :code-error
       :agent agent
;      :message (format fmt code msg (:error (if (string agent) (json/decode-from-str (string agent)) nil)))
       :code code
       )))
  true)

(defn create-job
  [conn queue job & options]
  (let [url (str "q/" queue)
        agent (agent-request url "POST" conn :body (json/encode-to-str job) :params options)]
    (check-return-code agent [201] "Error %s (%s) adding job: %s")
    true))

(defn reset-queue
  [conn queue]
  (let [agent (agent-request (str "q/" queue) "DELETE" conn)]
    (check-return-code agent [204] "Error %s (%s) resetting queue: %s")
    true))

(defn queue-size
  [conn queue]
  (let [agent (agent-request (str "status/" queue) "GET" conn)]
    (check-return-code agent [200] "Error %s (%s) getting  size of queue: %s")
    (json/decode-from-str (string agent))))

(defn fetch-job
  [conn queue & options]
  (let [agent (agent-request (str "q/" queue)
                             "GET"
                             conn
                             :params options)]
    (check-return-code agent [200 404] "Error %s (%s) getting job: %s")
    (let [body (json/decode-from-str (string agent))]
      (if (= (status agent) 200)
        body
        nil))))
