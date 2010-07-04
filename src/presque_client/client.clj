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
  (await-for 5000  agent)
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

(defn send-job
  [conn queue job method & options]
  (let [batch (if (vector? job) true false)
        url (str (if batch "qb/" "q/") queue)
        agent (agent-request url method conn :body (json/encode-to-str (if batch { :jobs job } job)) :params options)]
    (check-return-code agent [201] "Error %s (%s) adding job: %s")
    true))

(defn create-jobs
    [conn queue job & options]
    (send-job conn queue job "POST" options))

(defn create-job
  [conn queue job & options]
  (send-job conn queue job "POST" options))

(defn failed-job
  [conn queue job & options]
  (send-job conn queue job "PUT" options))

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

(defn get-jobs
  [conn queue batch-size & options]
  (let [batch (if (> batch-size 1) true false)
        params (if batch (conj options batch-size :batch_size) options)
        agent (agent-request (str (if batch "qb/" "q/") queue)
                             "GET"
                             conn
                             :params params)]
    (check-return-code agent [200 404] "Error %s (%s) getting job: %s")
    (let [body (json/decode-from-str (string agent))]
      (if (= (status agent) 200)
        body
        nil))))

(defn fetch-job
  [conn queue & options]
  (get-jobs conn queue 1 options))

(defn fetch-jobs
  [conn queue batch-size & options]
  (let [jobs-str (get-jobs conn queue batch-size options)]
    (if (nil? jobs-str)
      []
      ;; TODO - hack to decode json in json... fix later?
      (apply vector (map #(json/decode-from-str %) jobs-str )))))

(defn queue-status
  [conn queue]
  (let [agent (agent-request (str "control/" queue) "GET" conn)]
    (check-return-code agent [200] "Error %s (%s) getting queue status: %s")
    (json/decode-from-str (string agent))))

(defn change-queue-status
  [conn queue status]
  (let [agent (agent-request (str "control/" queue) "POST" conn :body (json/encode-to-str {:status status}))]
    (check-return-code agent [200] "Error %s (%s) getting  changing status of queue: %s")
    (json/decode-from-str (string agent))))

(defn start-queue
  [conn queue]
  (change-queue-status conn queue "start"))

(defn stop-queue
  [conn queue]
  (change-queue-status conn queue "stop"))

(defn queue-info
  [conn queue]
  (let [agent (agent-request (str "j/" queue) "GET" conn)]
    (check-return-code agent [200] "Error %s (%s) getting queue info: %s")
    (json/decode-from-str (string agent))))

(defn list-queues
  [conn]
  (let [agent (agent-request (str "status/") "GET" conn)]
    (check-return-code agent [200] "Error %s (%s) getting queues list: %s")
    (json/decode-from-str (string agent))))
