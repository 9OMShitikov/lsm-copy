package io

import (
	"github.com/pkg/errors"
)

var (
	// ErrObjNotFound may be returned from ReadObject in case requested file/object does not exists
	ErrObjNotFound = errors.New("Object doesn't exist")
)
