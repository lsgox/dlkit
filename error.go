package dlkit

import (
	"errors"
	"fmt"
	"net/http"
)

var (
	ErrBadLength      = errors.New("bad content length")
	ErrVerifyMismatch = errors.New("verify mismatch")
)

type StatusCodeError int

func (e StatusCodeError) Error() string {
	return fmt.Sprintf("http status error: %d %s", e, http.StatusText(int(e)))
}

func IsStatusCodeError(err error) bool {
	var e StatusCodeError
	return errors.As(err, &e)
}

type ErrChecksumMismatch struct {
	Expected string
	Actual   string
	Type     string
}

func (e *ErrChecksumMismatch) Error() string {
	return fmt.Sprintf("checksum %s mismatch: expected %s, got %s", e.Type, e.Expected, e.Actual)
}

func (e *ErrChecksumMismatch) Is(target error) bool {
	return errors.Is(target, ErrVerifyMismatch)
}

func IsChecksumMismatch(err error) bool {
	var e *ErrChecksumMismatch
	return errors.As(err, &e)
}

type ErrFileSizeMismatch struct {
	Expected int64
	Actual   int64
}

func (e *ErrFileSizeMismatch) Error() string {
	return fmt.Sprintf("file size mismatch: expected %d, got %d", e.Expected, e.Actual)
}

func (e *ErrFileSizeMismatch) Is(target error) bool {
	return errors.Is(target, ErrVerifyMismatch)
}

func IsFileSizeMismatch(err error) bool {
	var e *ErrFileSizeMismatch
	return errors.As(err, &e)
}

func IsVerifyError(err error) bool {
	return IsChecksumMismatch(err) || IsFileSizeMismatch(err)
}
