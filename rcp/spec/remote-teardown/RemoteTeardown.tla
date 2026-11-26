----------------------------- MODULE RemoteTeardown ----------------------------
EXTENDS FiniteSets, Naturals, Sequences
VARIABLE seen
vars == <<seen>>

(* Observer-level contract for one scenario: A carries data; B has no Ready
   at the source. Inject a short body only once both preconditions hold.
   Destination result and source EOF/result may race. *)
Prerequisites ==
  [e \in {"DataPrefix", "WaitB", "ShortData", "ControlEOF",
          "SourceError", "DestinationError"} |->
      CASE e = "ShortData" -> {"DataPrefix", "WaitB"}
        [] e = "ControlEOF" \/ e = "DestinationError" -> {"ShortData"}
        [] e = "SourceError" -> {"ControlEOF"}
        [] OTHER -> {}]
Events == DOMAIN Prerequisites
Complete == seen = Events

Init == seen = {}
Observe(e) ==
  /\ e \notin seen
  /\ Prerequisites[e] \subseteq seen
  /\ seen' = seen \cup {e}

Next ==
  \/ \E e \in Events: Observe(e)
  \/ Complete /\ UNCHANGED vars

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)
Terminates == <>Complete

(* Check one observed trace, in order. A missing event leaves an incomplete
   terminal trace; an illegal or out-of-order event deadlocks before the end. *)
TraceInit(position) == Init /\ position = 0
TraceEvent(event) == \E e \in Events: e = event /\ Observe(e)
TraceNext(position, trace) ==
  \/ /\ position < Len(trace)
     /\ TraceEvent(trace[position + 1])
     /\ position' = position + 1
  \/ position = Len(trace) /\ UNCHANGED <<vars, position>>

TraceComplete(position, trace) == position = Len(trace) => Complete
=============================================================================
