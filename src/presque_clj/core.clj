(ns presque-client.client
  (:use [clojure-http.client]
        [clojure.contrib.json]))

(defn connect-presque
  [& [url headers]]
  {:url (or url "http://localhost:5000/")
   :headers (or headers {:content-type "application/json"})})

(defn add-job
  [conn queue job]
  (let [res (request (str (:url conn) "q/" queue) "POST" (:headers conn) {} (json-str job))]
    res))

(defn get-job
  [conn queue]
  (let [res (request (str (:url conn) "q/" queue) "GET" (:headers conn))]
    res))

