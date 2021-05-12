package admin

import (
	"encoding/json"
	"fmt"
)

const (
	// errUserExists - Attempt to create existing user
	errUserExists errorReason = "UserAlreadyExists"

	// errNoSuchUser - Attempt to create existing user
	errNoSuchUser errorReason = "NoSuchUser"

	// errInvalidAccessKey - Invalid access key specified
	errInvalidAccessKey errorReason = "InvalidAccessKey"

	// errInvalidSecretKey - Invalid secret key specified
	errInvalidSecretKey errorReason = "InvalidSecretKey"

	// errInvalidKeyType - Invalid key type specified
	errInvalidKeyType errorReason = "InvalidKeyType"

	// errKeyExists - Provided access key exists and belongs to another user
	errKeyExists errorReason = "KeyExists"

	// errEmailExists - Provided email address exists
	errEmailExists errorReason = "EmailExists"

	// errInvalidCapability - Attempt to remove an invalid admin capability
	errInvalidCapability errorReason = "InvalidCapability"

	// errSubuserExists - Specified subuser exists
	errSubuserExists errorReason = "SubuserExists"

	// errInvalidAccess - Invalid subuser access specified
	errInvalidAccess errorReason = "InvalidAccess"

	// errIndexRepairFailed - Bucket index repair failed
	errIndexRepairFailed errorReason = "IndexRepairFailed"

	// errBucketNotEmpty - Attempted to delete non-empty bucket
	errBucketNotEmpty errorReason = "BucketNotEmpty"

	// errObjectRemovalFailed - Unable to remove objects
	errObjectRemovalFailed errorReason = "ObjectRemovalFailed"

	// errBucketUnlinkFailed - Unable to unlink bucket from specified user
	errBucketUnlinkFailed errorReason = "BucketUnlinkFailed"

	// errBucketLinkFailed - Unable to link bucket to specified user
	errBucketLinkFailed errorReason = "BucketLinkFailed"

	// errNoSuchObject - Specified object does not exist
	errNoSuchObject errorReason = "NoSuchObject"

	// errIncompleteBody - Either bucket was not specified for a bucket policy request or bucket and object were not specified for an object policy request.
	errIncompleteBody errorReason = "IncompleteBody"

	// errNoSuchCap - User does not possess specified capability
	errNoSuchCap errorReason = "NoSuchCap"

	// errInternalerror - Internal server error.
	errInternalerror errorReason = "Internalerror"

	// errAccessDenied - Access denied.
	errAccessDenied errorReason = "AccessDenied"

	// errNoSuchBucket - Bucket does not exist.
	errNoSuchBucket errorReason = "NoSuchBucket"

	// errNoSuchKey - No such access key.
	errNoSuchKey errorReason = "NoSuchKey"

	// errInvalidArgument - Invalid argument.
	errInvalidArgument errorReason = "InvalidArgument"

	// errUnknown - reports an unknown error
	errUnknown errorReason = "Unknown"
)

var (
	unmarshalWrapError = "failed to unmarshal radosgw bucket http response"
)

// errorReason is the reason of the error
type errorReason string

type statusError struct {
	Code      string `json:"Code,omitempty"`
	RequestID string `json:"RequestId,omitempty"`
	HostID    string `json:"HostId,omitempty"`
}

func handleStatusError(decodedResponse []byte) error {
	statusError := &statusError{}
	err := json.Unmarshal(decodedResponse, &statusError)
	if err != nil {
		return fmt.Errorf("%s. %s. %w", unmarshalWrapError, string(decodedResponse), err)
	}

	return radosGWError{
		code:      statusError.Code,
		requestID: statusError.RequestID,
		hostID:    statusError.HostID,
		error:     fmt.Sprintf("%s %s %s", statusError.Code, statusError.RequestID, statusError.HostID),
	}
}

// radosGWError Provides access to the body, error and model on returned errors.
type radosGWError struct {
	code      string
	requestID string
	hostID    string
	error     string
}

// error returns non-empty string if there was an error.
func (e radosGWError) Error() string {
	return e.error
}

func reasonForError(err error) errorReason {
	if err, ok := err.(radosGWError); ok {
		return errorReason(err.code)
	}

	return errUnknown
}

// IsUserExists returns a boolean indicating whether the error is known to
// report that a user exists
func IsUserExists(err error) bool {
	return reasonForError(err) == errUserExists
}

// IsNoSuchUser returns a boolean indicating whether the error is known to
// report that a user does not exist
func IsNoSuchUser(err error) bool {
	return reasonForError(err) == errNoSuchUser
}

// IsInvalidAccessKey returns a boolean indicating whether the error is known to
// report that an access key is invalid
func IsInvalidAccessKey(err error) bool {
	return reasonForError(err) == errInvalidAccessKey
}

// IsInvalidSecretKey returns a boolean indicating whether the error is known to
// report that a secret key is invalid
func IsInvalidSecretKey(err error) bool {
	return reasonForError(err) == errInvalidSecretKey
}

// IsInvalidKeyType returns a boolean indicating whether the error is known to
// report that the key type is invalid
func IsInvalidKeyType(err error) bool {
	return reasonForError(err) == errInvalidKeyType
}

// IsKeyExists returns a boolean indicating whether the error is known to
// report that an access key exists and belong to another user
func IsKeyExists(err error) bool {
	return reasonForError(err) == errKeyExists
}

// IsEmailExists returns a boolean indicating whether the error is known to
// report that the provided email for user already exists
func IsEmailExists(err error) bool {
	return reasonForError(err) == errEmailExists
}

// IsInvalidCapability returns a boolean indicating whether the error is known to
// report that an invalid capability was passed during a user creation
func IsInvalidCapability(err error) bool {
	return reasonForError(err) == errInvalidCapability
}

// IsSubuserExists returns a boolean indicating whether the error is known to
// report that a subuser exists
func IsSubuserExists(err error) bool {
	return reasonForError(err) == errSubuserExists
}

// IsInvalidAccess returns a boolean indicating whether the error is known to
// report that the subuser access specified is invalid
func IsInvalidAccess(err error) bool {
	return reasonForError(err) == errInvalidAccess
}

// IsIndexRepairFailed returns a boolean indicating whether the error is known to
// report that an index repair failed
func IsIndexRepairFailed(err error) bool {
	return reasonForError(err) == errIndexRepairFailed
}

// IsBucketNotEmpty returns a boolean indicating whether the error is known to
// report that a bucket is not empty
func IsBucketNotEmpty(err error) bool {
	return reasonForError(err) == errBucketNotEmpty
}

// IsObjectRemovalFailed returns a boolean indicating whether the error is known to
// report that the removal of an object failed
func IsObjectRemovalFailed(err error) bool {
	return reasonForError(err) == errObjectRemovalFailed
}

// IsBucketUnlinkFailed returns a boolean indicating whether the error is known to
// report that the server was unable to unlink bucket to specified user
func IsBucketUnlinkFailed(err error) bool {
	return reasonForError(err) == errBucketUnlinkFailed
}

// IsBucketLinkFailed returns a boolean indicating whether the error is known to
// report that the server was unable to link bucket to specified user
func IsBucketLinkFailed(err error) bool {
	return reasonForError(err) == errBucketLinkFailed
}

// IsNoSuchObject returns a boolean indicating whether the error is known to
// report that the specified object does not exist
func IsNoSuchObject(err error) bool {
	return reasonForError(err) == errNoSuchObject
}

// IsIncompleteBody returns a boolean indicating whether the error is known to
// report that either bucket was not specified for a bucket policy request or
// bucket and object were not specified for an object policy request.
func IsIncompleteBody(err error) bool {
	return reasonForError(err) == errIncompleteBody
}

// IsNoSuchCap returns a boolean indicating whether the error is known to
// report that the given  user does not possess specified capability
func IsNoSuchCap(err error) bool {
	return reasonForError(err) == errNoSuchCap
}

// IsInternalerror returns a boolean indicating whether the error is known to
// report that the server suffered from an internal error
func IsInternalerror(err error) bool {
	return reasonForError(err) == errInternalerror
}

// IsAccessDenied returns a boolean indicating whether the error is known to
// report that access to server is denied
func IsAccessDenied(err error) bool {
	return reasonForError(err) == errAccessDenied
}

// IsNoSuchBucket returns a boolean indicating whether the error is known to
// report that the given bucket does not exist
func IsNoSuchBucket(err error) bool {
	return reasonForError(err) == errNoSuchBucket
}

// IsNoSuchKey returns a boolean indicating whether the error is known to
// report that the given access key does not exist
func IsNoSuchKey(err error) bool {
	return reasonForError(err) == errNoSuchKey
}

// IsInvalidArgument returns a boolean indicating whether the error is known to
// report that an invalid argument was passed to the server
func IsInvalidArgument(err error) bool {
	return reasonForError(err) == errInvalidArgument
}
