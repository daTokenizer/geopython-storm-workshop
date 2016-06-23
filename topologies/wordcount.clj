(ns wordcount
  (:use     [streamparse.specs])
  (:gen-class))

(defn wordcount [options]
   [
    ;; spout configuration
    {"location-spout" (python-spout-spec
          options
          "spouts.locations.LocationSpout"
          ["location"]
          )
    }
    ;; bolt configuration
    {"topx-bolt" (python-bolt-spec ;; FILTER
          options
          { "location-spout" :shuffle }
          "bolts.matcher.TopX"
          [ "top_x" "location"]
          :p 2
          )
     
    ;; bolt configuration
     "rank-bolt" (python-bolt-spec ;; SPLITTER!
        options
        { "topx-bolt" :shuffle }
        "bolts.ranker.Ranker"
        []
        ;   { "output" ["location_a" "location_b"]
        ;     "dehydration-order" ["location" "timeout"] }
        ;   :p 2
        )
    }
  ]
)


;; bolt configuration
; {"count-bolt" (python-bolt-spec
;       options
;       { ["topx-bolt" "output"] :shuffle }
;       "bolts.wordcount.WordCounter"
;       ["word" "count"]
;       :p 2
;       )
; }




    ; {"dehydrator-bolt" (python-bolt-spec
    ;       ;; topology options passed in
    ;       options
    ;       ;; inputs, where does this bolt recieve it's tuples from? can specify multiple
    ;       {["rank-bolt" "dehydration-order"] :shuffle}
    ;       ;; class to run
    ;       "bolts.dehydrator.DehydratorBolt"
    ;       ;; output spec, what tuples does this bolt emit?
    ;       ["location"]
    ;       ;;:p 2
    ;       )
    ; }
;   ]
; )
