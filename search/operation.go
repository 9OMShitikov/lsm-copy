package search

import (
	"encoding/json"
	"strings"

	"github.com/pkg/errors"
)

// Operation specifies boolean operation on query operands
type Operation int

// boolean operations
const (
	_ = Operation(iota)
	AND
	OR
	NOR
)

// String implements fmt.Stringer
func (o Operation) String() string {
	switch o {
	case AND:
		return "AND"
	case OR:
		return "OR"
	case NOR:
		return "NOR"
	default:
		return "UNKNOWN_OPERATION"
	}
}

// OperationFromString builds Operation from string representation
func OperationFromString(s string) (Operation, error) {
	s = strings.ToUpper(s)
	switch s {
	case "AND":
		return AND, nil
	case "OR":
		return OR, nil
	case "NOR":
		return NOR, nil
	default:
		return 0, errors.Errorf("unknown operation '%s'", s)
	}
}

// MarshalJSON implements json.Marshaler
func (o Operation) MarshalJSON() ([]byte, error) {
	return []byte(`"` + o.String() + `"`), nil
}

// UnmarshalJSON implements json.Unmarshaler
func (o *Operation) UnmarshalJSON(b []byte) (err error) {
	var s string
	if err = json.Unmarshal(b, &s); err != nil {
		return
	}
	*o, err = OperationFromString(s)
	return
}
