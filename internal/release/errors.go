package release

import "errors"

var (
	// ErrExpired is returned when an action is attempted after the validity
	// deadline.
	ErrExpired = errors.New("release has expired")
	// ErrSameOperator is returned when the reviewing duty operator is the same
	// person as the originator; two distinct operators are mandatory.
	ErrSameOperator = errors.New("reviewer must be a different duty operator")
	// ErrAlreadyConsumed is returned on any attempt to drive a valve with a
	// release that has already been consumed (concurrent or post-restart).
	ErrAlreadyConsumed = errors.New("release has already been consumed")
	// ErrNotEndorsed is returned when execution is attempted before a valid
	// countersignature exists.
	ErrNotEndorsed = errors.New("release is not endorsed")
	// ErrParameterMismatch is returned when live parameters no longer match the
	// release's bound parameter set.
	ErrParameterMismatch = errors.New("release parameters no longer match live state")
	// ErrInvalidCredential is returned when an operator presents a missing or
	// wrong independent credential (originator or reviewer).
	ErrInvalidCredential = errors.New("invalid operator credential")
)
