(defpackage :spread
  (:nicknames :spread-gcs)
  (:use :common-lisp
	#+clisp :ffi
	)
  (:export
   #:UNRELIABLE_MESS
   #:RELIABLE_MESS
   #:FIFO_MESS
   #:CAUSAL_MESS
   #:AGREED_MESS
   #:SAFE_MESS
   #:REGULAR_MESS
   #:SELF_DISCARD
   #:DROP_RECV
   #:REG_MEMB_MESS
   #:TRANSITION_MESS
   #:CAUSED_BY_JOIN
   #:CAUSED_BY_LEAVE
   #:CAUSED_BY_DISCONNECT
   #:CAUSED_BY_NETWORK
   #:MEMBERSHIP_MESS
   #:ENDIAN_RESERVED
   #:RESERVED
   #:REJECT_MESS
   #:ACCEPT_SESSION
   #:ILLEGAL_SPREAD
   #:COULD_NOT_CONNECT
   #:REJECT_QUOTA
   #:REJECT_NO_NAME
   #:REJECT_ILLEGAL_NAME
   #:REJECT_NOT_UNIQUE
   #:REJECT_VERSION
   #:CONNECTION_CLOSED
   #:REJECT_AUTH
   #:ILLEGAL_SESSION
   #:ILLEGAL_SERVICE
   #:ILLEGAL_MESSAGE
   #:ILLEGAL_GROUP
   #:BUFFER_TOO_SHORT
   #:GROUPS_TOO_SHORT
   #:MESSAGE_TOO_LONG
   #:NET_ERROR_ON_SESSION

   #:connect
   ))
(in-package :spread)

(defvar *spreadlibrary* "/home/cbbrowne/spread/spread-4.0.0/lib/libspread.so")

(defconstant UNRELIABLE_MESS  #x00000001)
(defconstant RELIABLE_MESS #x00000002)
(defconstant FIFO_MESS #x00000004)
(defconstant CAUSAL_MESS #x00000008)
(defconstant AGREED_MESS #x00000010)
(defconstant SAFE_MESS #x00000020)
(defconstant REGULAR_MESS #x0000003f)

(defconstant SELF_DISCARD #x00000040)
(defconstant DROP_RECV #x01000000)

(defconstant REG_MEMB_MESS #x00001000)
(defconstant TRANSITION_MESS #x00002000)
(defconstant CAUSED_BY_JOIN #x00000100)
(defconstant CAUSED_BY_LEAVE #x00000200)
(defconstant CAUSED_BY_DISCONNECT #x00000400)
(defconstant CAUSED_BY_NETWORK #x00000800)
(defconstant MEMBERSHIP_MESS #x00003f00)

(defconstant ENDIAN_RESERVED #x80000080)
(defconstant RESERVED #x003fc000)
(defconstant REJECT_MESS #x00400000)

(defconstant ACCEPT_SESSION 1)
(defconstant ILLEGAL_SPREAD -1)
(defconstant COULD_NOT_CONNECT -2)
(defconstant REJECT_QUOTA -3)
(defconstant REJECT_NO_NAME -4)
(defconstant REJECT_ILLEGAL_NAME -5)
(defconstant REJECT_NOT_UNIQUE -6)
(defconstant REJECT_VERSION -7)
(defconstant CONNECTION_CLOSED -8)
(defconstant REJECT_AUTH -9)

(defconstant ILLEGAL_SESSION -11)
(defconstant ILLEGAL_SERVICE -12)
(defconstant ILLEGAL_MESSAGE -13)
(defconstant ILLEGAL_GROUP -14)
(defconstant BUFFER_TOO_SHORT -15)
(defconstant GROUPS_TOO_SHORT -16)
(defconstant MESSAGE_TOO_LONG -17)
(defconstant NET_ERROR_ON_SESSION -18)

(defconstant MAX_GROUP_NAME 32)

(FFI:DEFAULT-FOREIGN-LIBRARY *spreadlibrary*)

(ffi:def-call-out sp-connect
		  (:name "SP_connect")
		  (:language :stdc)
		  (:arguments
		   (spreadname ffi:c-string)
		   (privatename ffi:c-string)
		   (priority ffi:int)
		   (groupmembership ffi:int)
		   (mailbox (ffi:c-ptr ffi:int) :out)
		   (private-group (ffi:c-ptr ffi:char) :out))
		  (:return-type ffi:int))

(defun connect (&key spread-host (type 'string)
		     connection-name (type 'string)
		     priority (type 't)
		     groupmembership (type 't))
  (multiple-value-bind (rc mailbox unicast-group)
      (sp-connect spread-host connection-name (if priority 1 0) (if groupmembership 1 0))
    (cond
     ((= rc ILLEGAL_SPREAD)
      (error "ILLEGAL_SPREAD - ~S not a valid spread server address" spread-host))
     ((= rc COULD_NOT_CONNECT)
      (error "COULD_NOT_CONNECT - ~S not accepting connections" spread-host))
     ((= rc CONNECTION_CLOSED)
      (error "CONNECTION_CLOSED - ~S closed connection" spread-host))
     ((= rc REJECT_VERSION)
      (error "REJECT_VERSION - ~S rejected connection due to Spread version mismatch" spread-host))
     ((= rc REJECT_NO_NAME)
      (error "REJECT_NO_NAME - ~S rejected connection as length of name ~S was zero"  spread-host connection-name))
     ((= rc REJECT_ILLEGAL_NAME)
      (error "REJECT_ILLEGAL_NAME - connection name ~S rejected - too long/illegal chars" connection-name))
     ((= rc REJECT_NOT_UNIQUE)
      (error "REJECT_NOT_UNIQUE - connection name ~S rejected - name already in use on ~S" connection-name spread-host)))       
    (values mailbox unicast-group)))

(ffi:def-call-out sp-disconnect
		  (:name "SP_disconnect")
		  (:language :stdc)
		  (:arguments
		   (mailbox ffi:int))
		  (:return-type ffi:int))

(defun disconnect (mailbox)
  (let ((rc (sp-disconnect mailbox)))
    (cond
     ((= rc ILLEGAL_SESSION)
      (error "ILLEGAL_SESSION - no such session!")))))

(ffi:def-call-out sp-join
		  (:name "SP_join")
		  (:language :stdc)
		  (:arguments
		   (mailbox ffi:int)
		   (group ffi:c-string))
		  (:return-type ffi:int))

(defun join (mbox group)
  (let ((rc (sp-join mbox group)))
    (cond
     ((= rc ILLEGAL_GROUP)
      (error "ILLEGAL_GROUP - group name ~S is illegal~%" group))
     ((= rc ILLEGAL_SESSION)
      (error "ILLEGAL_SESSION - session ~A is invalid~%" mbox))
     ((= rc CONNECTION_CLOSED)
      (error "CONNECTION_CLOSED - connection closed, could not join group~%")))))

(ffi:def-call-out spread-leave
		  (:name "SP_leave")
		  (:language :stdc)
		  (:arguments
		   (mailbox ffi:int)
		   (group ffi:c-string))
		  (:return-type ffi:int))

(defun leave (mbox group)
  (let ((rc (sp-leave mbox group)))
    (cond
     ((= rc ILLEGAL_GROUP)
      (error "ILLEGAL_GROUP - group name ~S is illegal~%" group))
     ((= rc ILLEGAL_SESSION)
      (error "ILLEGAL_SESSION - session ~A is invalid~%" mbox))
     ((= rc CONNECTION_CLOSED)
      (error "CONNECTION_CLOSED - connection closed, could not leave group~%")))))

;;;int SP_multigroup_multicast (mailbox mbox, service service_type, int num_groups,
;;;  const char groups[][MAX_GROUP_NAME], int16 mess_type,
;;;  int mess_len, const char *mess);

;(ffi:def-call-out spread-multicast
; (:name "SP_multicast")
; (:language :stdc)
; (:arguments
; (mailbox ffi:int)
; (service-type ffi:int)
; (group ffi:c-string :out)
; (message-type ffi:sint16)
; (message-length ffi:int)
; (message ffi:c-string))
; (:return-type ffi:int))

(ffi:def-call-out sp-multicast
		  (:name "SP_multicast")
		  (:language :stdc)
		  (:arguments
		   (mailbox ffi:int)
		   (service-type ffi:int)
		   (group ffi:c-string)
		   (message-type ffi:sint16)
		   (message-length ffi:int)
		   (message ffi:c-string))
		  (:return-type ffi:int))


;int SP_multigroup_multicast (mailbox mbox, service service_type, int num_groups,
;                             const char groups[][MAX_GROUP_NAME], int16 mess_type,
;                             int mess_len, const char *mess);

(ffi:def-call-out sp-multigroup-multicast
		  (:name "SP_multigroup_multicast")
		  (:language :stdc)
		  (:arguments
		   (mailbox ffi:int)
		   (service-type ffi:int)
		   (num-groups ffi:int)
		   (group (ffi:c-ptr ffi:c-string))
		   (message-type ffi:sint16)
		   (message-length ffi:int)
		   (message ffi:c-string))
		  (:return-type ffi:int))

(defun multicast (&key mailbox (type 'integer)
		       service-type (type 'integer)
		       group (type 'string)
		       message-type (type 'integer)
		       message (type 'string))
  (if (member service-type (list UNRELIABLE_MESS RELIABLE_MESS FIFO_MESS CAUSAL_MESS AGREED_MESS SAFE_MESS REGULAR_MESS))
      nil
    (error "invalid service type ~A - must be one of UNRELIABLE_MESS RELIABLE_MESS FIFO_MESS CAUSAL_MESS AGREED_MESS SAFE_MESS REGULAR_MESS" service-type))
  (let ((rc (sp-multicast mailbox service-type group message-type (length message) message)))
    (cond
     ((= rc ILLEGAL_SESSION)
      (error "ILLEGAL_SESSION - mbox ~D was illegal" mailbox))
     ((= rc ILLEGAL_SESSION)
      (error "ILLEGAL_MESSAGE - message had an illegal structure"))
     ((= rc CONNECTION_CLOSED)
      (error "CONNECTION_CLOSED - error during communications")))
    rc))

;int SP_receive (mailbox mbox, service *service_type, char sender[MAX_GROUP_NAME], 
; int max_groups, int *num_groups, char groups[][MAX_GROUP_NAME], 
; int16 *mess_type, int *endian_mismatch, int max_mess_len, char *mess);

(ffi:def-call-out spread-receive
		  (:name "SP_receive")
		  (:language :stdc)
		  (:arguments
		   (mailbox ffi:int)
		   (service-type (ffi:c-ptr ffi:int) :out)
		   (sender (ffi:c-ptr ffi:char) :out)
		   (max-groups ffi:int)
		   (num-groups (ffi:c-ptr ffi:int) :out)
		   (ffi:def-c-const MAX_GROUP_NAME)
		   (groups (ffi:c-array ffi:c-string 32)) :out)
		   (msg-type (ffi:c-ptr ffi:sint16) :in-out)
		   (endian-mismatch (ffi:c-ptr ffi:int) :in-out)
		   (max-msg-len ffi:int)
		   (message (ffi:c-ptr ffi:char) :out))
		  (:return-type ffi:int))

(defun connect-and-send (msg group reliability)
  (let ((server "4803@dba2")
	(user "clisp-client"))
    (multiple-value-bind
	(mailbox unicast-group)
	(connect :spread-host server :connection-name user)
      (format t "Connected to server ~A as ~A - mailbox=~D" server user mailbox)
      (join mailbox group)
;;;;; need to capture multiple values from this
      (format t "Send message ~A" msg)
      (multicast mailbox reliability group 1 msg)
      (leave mailbox group)
      (disconnect mailbox))))

(defun connect-send-messages (format group reliability count)
  (let ((server "4803@dba2")
	(user "clisp-mclient"))
    (multiple-value-bind
	(mailbox unicast-group)
	(connect :spread-host server :connection-name user)
      (format t "Connected to server ~A as ~A - mailbox=~D" server user mailbox)
;;;;; need to capture multiple values from this
      (loop for i from 1 to count
	do (multicast :mailbox mailbox :service-type reliability :group group :message-type 1 :message (format nil format i)))
      (disconnect mailbox))))

(defun connect-and-get-msg (group)
  (let ((server "4803@dba2")
	(user "clisp-reader"))
    (multiple-value-bind
	(mailbox unicast-group)
	(connect :spread-host server :connection-name user)
      (format t "Connected to server ~A as ~A - mailbox=~D" server user mailbox)
      (join mailbox group)
      (multiple-value-bind
	  (rc service-type sender num-groups groups msg-type endian-mismatch message)
	  (spread-receive mailbox 32 1 1 3000)
	(format t "receipt: ~A ~A ~A ~A ~A ~A ~A ~A~%" rc service-type sender num-groups groups msg-type endian-mismatch message)))))