package codeerrors

import "fmt"

// Error wraps implementation-specific error with code
type Error struct {
	Code    string
	Message string
	Reason  error
}

// Error implements standard error interface
func (e Error) Error() string {
	if e.Reason != nil {
		return e.Reason.Error()
	}
	msg := e.Message + " (code=" + string(e.Code) + ")"
	return msg
}

// Cause implements errors.Causer
func (e Error) Cause() error {
	return e.Reason
}

// Unwrap provides compatibility with Go 1.13+ error chains
func (e Error) Unwrap() error {
	return e.Reason
}

// Is consults Go1.13+ errors.Is
func (e Error) Is(target error) bool {
	if tErr, ok := target.(Error); ok {
		return e.Code == tErr.Code
	}
	if tErr, ok := target.(*Error); ok {
		return e.Code == tErr.Code
	}
	return false
}

// WithMessage returns an error with formatted message
func (e Error) WithMessage(msg string, args ...interface{}) Error {
	e.Message = fmt.Sprintf(msg, args...)
	return e
}

// WithReason returns cloned error with given reason
func (e Error) WithReason(err error) Error {
	e.Reason = err
	return e
}

// Wrap returns cloned error with given reason
func (e Error) Wrap(err error) error {
	if e.Reason == nil {
		e.Reason = err
	} else {
		if er, ok := e.Reason.(interface{ Wrap(error) error }); ok {
			e.Reason = er.Wrap(err)
		} else {
			panic("re-wrapping error not supporting Wrap()")
		}
	}
	return e
}
