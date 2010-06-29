(ns presque-client.client
  (:use [clojure.contrib.http.agent :exclude [bytes]]
        [clojure.contrib.json]
        [clojure.contrib.condition]))

(defn connect-presque
  [& [url headers]]
  {:base-url (or url "http://localhost:5000/")
   :headers (or headers {"Content-Type" "application/json"})
   :connect-timeout 1000
   :read-timeout 5000})

(defn agent-request
  [path method conn & [body]]
  (let [url (str (:base-url conn) path)]
    (http-agent url
                :method method
                :body body
                :headers (:headers conn)
                :connection-timeout (:connection-timeout conn)
                :read-timeout (:read-timeout conn))))

(defn read-json-body
  [body]
  (read-json body true false nil))

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
    (print code "\n")
    (if-not (some #(= % code) expected)
      (raise
       :type :presque-error
       :agent agent
;       :message (format fmt code msg (:error (read-json-body (string agent))))
       )))
  true)

(defn add-job
  [conn queue job]
  (let [agent (agent-request (str "q/" queue) "POST" conn (json-str job))]
    (check-return-code agent [201] "Error %s (%s) adding job: %s")
    true))

(defn reset-queue
  [conn queue]
  (let [agent (agent-request (str "q/" queue) "DELETE" conn)]
    (check-return-code agent [204] "Error %s (%s) resetting queue: %s")
    true))

(defn queue-info
  [conn queue]
  (let [agent (agent-request (str "j/" queue) "GET" conn)]
    (check-return-code agent [204] "Error %s (%s) getting info on queue: %s")
    true))

(defn get-job
  [conn queue]
  (let [agent (agent-request (str "q/" queue)
                             "GET"
                             conn)]
    (check-return-code agent [200 404] "Error %s (%s) getting job: %s")
    (await-for 1000 agent)
    (let [body (read-json-body (string agent))]
      (if (= (status agent) 200)
        body
        nil))))

