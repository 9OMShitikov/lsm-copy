package codeerrors

// codes
const (
	CodeConflict   = "conflict"
	CodeNotFound   = "not_found"
	CodeOutOfRange = "out_of_range"
)

// predefined errors
var(
	ErrConflict   = Error{Code: CodeConflict}
	ErrNotFound   = Error{Code: CodeNotFound}
	ErrOutOfRange = Error{Code: CodeOutOfRange}
)
