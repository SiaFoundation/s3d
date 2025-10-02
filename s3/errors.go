package s3

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
)

// S3Error implements error and carries the canonical S3 error code,
// a short description, and the HTTP status code returned by S3.
//
// For a few legacy/SOAP-only rows where the AWS table shows HTTP status "N/A",
// the errors were added but commented out
type S3Error struct {
	Code        string
	Description string
	HTTPStatus  int
}

// Error implements the error interface.
func (e S3Error) Error() string {
	return fmt.Sprintf("%s (%d): %s", e.Code, e.HTTPStatus, e.Description)
}

// The following errors are taken from the official list of errors here:
// https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
//
// The list should be complete apart for the following errors which were listed
// without a status code:
// ErrInvalidAddressingHeader                        = S3Error{"InvalidAddressingHeader", "You must specify the Anonymous role.", 0}
var (
	ErrAccessControlListNotSupported                  = S3Error{"AccessControlListNotSupported", "The bucket does not allow ACLs.", http.StatusBadRequest}
	ErrAccessDenied                                   = S3Error{"AccessDenied", "Access denied.", http.StatusForbidden}
	ErrAccessPointAlreadyOwnedByYou                   = S3Error{"AccessPointAlreadyOwnedByYou", "An access point with the same name already exists in your account.", http.StatusConflict}
	ErrAccountProblem                                 = S3Error{"AccountProblem", "There is a problem with your AWS account preventing the operation.", http.StatusForbidden}
	ErrAllAccessDisabled                              = S3Error{"AllAccessDisabled", "All access to this Amazon S3 resource has been disabled.", http.StatusForbidden}
	ErrAmbiguousGrantByEmailAddress                   = S3Error{"AmbiguousGrantByEmailAddress", "The provided email address maps to more than one account.", http.StatusBadRequest}
	ErrAuthorizationHeaderMalformed                   = S3Error{"AuthorizationHeaderMalformed", "The Authorization header is not valid.", http.StatusBadRequest}
	ErrAuthorizationQueryParametersError              = S3Error{"AuthorizationQueryParametersError", "The authorization query parameters are not valid.", http.StatusBadRequest}
	ErrBadDigest                                      = S3Error{"BadDigest", "Provided Content-MD5/checksum does not match what the server received.", http.StatusBadRequest}
	ErrBucketAlreadyExists                            = S3Error{"BucketAlreadyExists", "Requested bucket name is not available; the namespace is shared.", http.StatusConflict}
	ErrBucketAlreadyOwnedByYou                        = S3Error{"BucketAlreadyOwnedByYou", "Bucket already exists and is owned by you.", http.StatusConflict}
	ErrBucketHasAccessPointsAttached                  = S3Error{"BucketHasAccessPointsAttached", "Bucket to delete has access points attached; delete them first.", http.StatusBadRequest}
	ErrBucketNotEmpty                                 = S3Error{"BucketNotEmpty", "Bucket you tried to delete is not empty.", http.StatusConflict}
	ErrClientTokenConflict                            = S3Error{"ClientTokenConflict", "Multi-Region Access Point idempotency token was already used for a different request.", http.StatusConflict}
	ErrConnectionClosedByRequester                    = S3Error{"ConnectionClosedByRequester", "Error while reading WriteGetObjectResponse body; returned to original caller.", http.StatusBadRequest}
	ErrConditionalRequestConflict                     = S3Error{"ConditionalRequestConflict", "A conflicting operation occurred (e.g., during PutObject or MPU).", http.StatusConflict}
	ErrCredentialsNotSupported                        = S3Error{"CredentialsNotSupported", "This request does not support credentials.", http.StatusBadRequest}
	ErrCrossLocationLoggingProhibited                 = S3Error{"CrossLocationLoggingProhibited", "Cross-Region logging is not allowed.", http.StatusForbidden}
	ErrDeviceNotActiveError                           = S3Error{"DeviceNotActiveError", "The device is not currently active.", http.StatusBadRequest}
	ErrEndpointNotFound                               = S3Error{"EndpointNotFound", "Direct requests to the correct endpoint.", http.StatusBadRequest}
	ErrEntityTooSmall                                 = S3Error{"EntityTooSmall", "Proposed upload is smaller than the minimum object size.", http.StatusBadRequest}
	ErrEntityTooLarge                                 = S3Error{"EntityTooLarge", "Proposed upload exceeds the maximum allowed object size.", http.StatusBadRequest}
	ErrExpiredToken                                   = S3Error{"ExpiredToken", "The provided token has expired.", http.StatusBadRequest}
	ErrIllegalLocationConstraintException             = S3Error{"IllegalLocationConstraintException", "Bucket Region mismatch or wrong endpoint for the location constraint.", http.StatusBadRequest}
	ErrIllegalVersioningConfigurationException        = S3Error{"IllegalVersioningConfigurationException", "The specified versioning configuration is not valid.", http.StatusBadRequest}
	ErrIncompleteBody                                 = S3Error{"IncompleteBody", "Bytes sent do not match the Content-Length header.", http.StatusBadRequest}
	ErrIncorrectEndpoint                              = S3Error{"IncorrectEndpoint", "Bucket exists in another Region; use the correct endpoint.", http.StatusBadRequest}
	ErrIncorrectNumberOfFilesInPostRequest            = S3Error{"IncorrectNumberOfFilesInPostRequest", "POST requires exactly one file upload per request.", http.StatusBadRequest}
	ErrInlineDataTooLarge                             = S3Error{"InlineDataTooLarge", "Inline data exceeds the maximum allowed size.", http.StatusBadRequest}
	ErrInternalError                                  = S3Error{"InternalError", "An internal error occurred; try again.", http.StatusInternalServerError}
	ErrInvalidAccessKeyId                             = S3Error{"InvalidAccessKeyId", "Provided AWS access key ID does not exist in records.", http.StatusForbidden}
	ErrInvalidAccessPoint                             = S3Error{"InvalidAccessPoint", "Specified access point name or account is not valid.", http.StatusBadRequest}
	ErrInvalidAccessPointAliasError                   = S3Error{"InvalidAccessPointAliasError", "Specified access point alias name is not valid.", http.StatusBadRequest}
	ErrInvalidArgument                                = S3Error{"InvalidArgument", "Argument/header invalid, missing, malformed, or too short (≥3 required).", http.StatusBadRequest}
	ErrInvalidBucketAclWithObjectOwnership            = S3Error{"InvalidBucketAclWithObjectOwnership", "Bucket ACLs cannot be set when ObjectOwnership=BucketOwnerEnforced.", http.StatusBadRequest}
	ErrInvalidBucketName                              = S3Error{"InvalidBucketName", "The specified bucket name is not valid.", http.StatusBadRequest}
	ErrInvalidBucketOwnerAWSAccountID                 = S3Error{"InvalidBucketOwnerAWSAccountID", "Expected bucket owner must be an AWS account ID.", http.StatusBadRequest}
	ErrInvalidBucketState                             = S3Error{"InvalidBucketState", "Request is not valid for the bucket’s current state.", http.StatusConflict}
	ErrInvalidDigest                                  = S3Error{"InvalidDigest", "Provided Content-MD5/checksum is not valid.", http.StatusBadRequest}
	ErrInvalidEncryptionAlgorithmError                = S3Error{"InvalidEncryptionAlgorithmError", "Encryption request is not valid; valid value is AES256.", http.StatusBadRequest}
	ErrInvalidHostHeader                              = S3Error{"InvalidHostHeader", "Host headers used the incorrect addressing style.", http.StatusBadRequest}
	ErrInvalidHttpMethod                              = S3Error{"InvalidHttpMethod", "Request used an unexpected HTTP method.", http.StatusBadRequest}
	ErrInvalidLocationConstraint                      = S3Error{"InvalidLocationConstraint", "The specified Region constraint is not valid.", http.StatusBadRequest}
	ErrInvalidObjectState                             = S3Error{"InvalidObjectState", "Operation is not valid for the object's current state.", http.StatusForbidden}
	ErrInvalidPart                                    = S3Error{"InvalidPart", "One or more parts not found or ETag did not match.", http.StatusBadRequest}
	ErrInvalidPartOrder                               = S3Error{"InvalidPartOrder", "Parts list is not in ascending part-number order.", http.StatusBadRequest}
	ErrInvalidPayer                                   = S3Error{"InvalidPayer", "All access to this object has been disabled.", http.StatusForbidden}
	ErrInvalidPolicyDocument                          = S3Error{"InvalidPolicyDocument", "Form content does not meet the policy document conditions.", http.StatusBadRequest}
	ErrInvalidRange                                   = S3Error{"InvalidRange", "Requested byte range cannot be satisfied.", http.StatusRequestedRangeNotSatisfiable}
	ErrInvalidRegion                                  = S3Error{"InvalidRegion", "Attempted to create a Multi-Region Access Point in a Region you haven't opted in to.", http.StatusForbidden}
	ErrInvalidRequest                                 = S3Error{"InvalidRequest", "The request is invalid (see docs for common causes including signature version, pagination, lifecycle, acceleration, conflicts, etc.).", http.StatusBadRequest}
	ErrInvalidSessionException                        = S3Error{"InvalidSessionException", "Session no longer exists because it timed out or expired.", http.StatusBadRequest}
	ErrInvalidSignature                               = S3Error{"InvalidSignature", "Server-calculated signature does not match the provided signature.", http.StatusBadRequest}
	ErrInvalidSecurity                                = S3Error{"InvalidSecurity", "Provided security credentials are not valid.", http.StatusForbidden}
	ErrInvalidSOAPRequest                             = S3Error{"InvalidSOAPRequest", "SOAP request body is not valid.", http.StatusBadRequest}
	ErrInvalidStorageClass                            = S3Error{"InvalidStorageClass", "The specified storage class is not valid.", http.StatusBadRequest}
	ErrInvalidTargetBucketForLogging                  = S3Error{"InvalidTargetBucketForLogging", "Logging target bucket missing, not owned by you, or lacks required grants.", http.StatusBadRequest}
	ErrInvalidToken                                   = S3Error{"InvalidToken", "Provided token is malformed or otherwise invalid.", http.StatusBadRequest}
	ErrInvalidURI                                     = S3Error{"InvalidURI", "Specified URI could not be parsed.", http.StatusBadRequest}
	ErrKeyTooLongError                                = S3Error{"KeyTooLongError", "The object key is too long.", http.StatusBadRequest}
	ErrKMSDisabledException                           = S3Error{"KMS.DisabledException", "Specified KMS key is disabled.", http.StatusBadRequest}
	ErrKMSInvalidKeyUsageException                    = S3Error{"KMS.InvalidKeyUsageException", "KMS KeyUsage or algorithm is incompatible with the operation/key type.", http.StatusBadRequest}
	ErrKMSInvalidStateException                       = S3Error{"KMS.KMSInvalidStateException", "Resource/key state not valid for the request.", http.StatusBadRequest}
	ErrKMSNotFoundException                           = S3Error{"KMS.NotFoundException", "Specified KMS entity or resource was not found.", http.StatusBadRequest}
	ErrMalformedACLError                              = S3Error{"MalformedACLError", "Provided ACL is not well-formed or fails schema validation.", http.StatusBadRequest}
	ErrMalformedPOSTRequest                           = S3Error{"MalformedPOSTRequest", "POST body is not well-formed multipart/form-data.", http.StatusBadRequest}
	ErrMalformedXML                                   = S3Error{"MalformedXML", "Provided XML is not well-formed or fails schema validation.", http.StatusBadRequest}
	ErrMalformedPolicy                                = S3Error{"MalformedPolicy", "Your policy contains a principal that is not valid.", http.StatusBadRequest}
	ErrMaxMessageLengthExceeded                       = S3Error{"MaxMessageLengthExceeded", "Request was too large.", http.StatusBadRequest}
	ErrMaxPostPreDataLengthExceededError              = S3Error{"MaxPostPreDataLengthExceededError", "POST fields preceding the file were too large.", http.StatusBadRequest}
	ErrMetadataTooLarge                               = S3Error{"MetadataTooLarge", "Metadata headers exceed the maximum allowed size.", http.StatusBadRequest}
	ErrMethodNotAllowed                               = S3Error{"MethodNotAllowed", "The specified method is not allowed for this resource.", http.StatusMethodNotAllowed}
	ErrMissingAttachment                              = S3Error{"MissingAttachment", "A SOAP attachment was expected but not found.", http.StatusBadRequest}
	ErrMissingAuthenticationToken                     = S3Error{"MissingAuthenticationToken", "The request was not signed.", http.StatusForbidden}
	ErrMissingContentLength                           = S3Error{"MissingContentLength", "Content-Length header is required.", http.StatusLengthRequired}
	ErrMissingRequestBodyError                        = S3Error{"MissingRequestBodyError", "An empty XML document was sent as the request.", http.StatusBadRequest}
	ErrMissingSecurityElement                         = S3Error{"MissingSecurityElement", "SOAP 1.1 request is missing a security element.", http.StatusBadRequest}
	ErrMissingSecurityHeader                          = S3Error{"MissingSecurityHeader", "Request is missing a required header.", http.StatusBadRequest}
	ErrNoLoggingStatusForKey                          = S3Error{"NoLoggingStatusForKey", "No logging status subresource exists for a key.", http.StatusBadRequest}
	ErrNoSuchAccessPoint                              = S3Error{"NoSuchAccessPoint", "The specified access point does not exist.", http.StatusNotFound}
	ErrNoSuchAsyncRequest                             = S3Error{"NoSuchAsyncRequest", "The specified request was not found.", http.StatusNotFound}
	ErrNoSuchBucket                                   = S3Error{"NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound}
	ErrNoSuchBucketPolicy                             = S3Error{"NoSuchBucketPolicy", "The specified bucket does not have a bucket policy.", http.StatusNotFound}
	ErrNoSuchCORSConfiguration                        = S3Error{"NoSuchCORSConfiguration", "The specified bucket does not have a CORS configuration.", http.StatusNotFound}
	ErrNoSuchKey                                      = S3Error{"NoSuchKey", "The specified key does not exist.", http.StatusNotFound}
	ErrNoSuchLifecycleConfiguration                   = S3Error{"NoSuchLifecycleConfiguration", "The specified lifecycle configuration does not exist.", http.StatusNotFound}
	ErrNoSuchMultiRegionAccessPoint                   = S3Error{"NoSuchMultiRegionAccessPoint", "The specified Multi-Region Access Point does not exist.", http.StatusNotFound}
	ErrNoSuchObjectLockConfiguration                  = S3Error{"NoSuchObjectLockConfiguration", "The specified object does not have an Object Lock configuration.", http.StatusNotFound}
	ErrNoSuchTagSet                                   = S3Error{"NoSuchTagSet", "The specified tag does not exist.", http.StatusNotFound}
	ErrNoSuchUpload                                   = S3Error{"NoSuchUpload", "Specified multipart upload does not exist (invalid ID, aborted, or completed).", http.StatusNotFound}
	ErrNoSuchVersion                                  = S3Error{"NoSuchVersion", "Specified version ID does not match an existing version.", http.StatusNotFound}
	ErrNoSuchWebsiteConfiguration                     = S3Error{"NoSuchWebsiteConfiguration", "The specified bucket does not have a website configuration.", http.StatusNotFound}
	ErrNoTransformationDefined                        = S3Error{"NoTransformationDefined", "No transformation found for this Object Lambda Access Point.", http.StatusNotFound}
	ErrNotDeviceOwnerError                            = S3Error{"NotDeviceOwnerError", "The device that generated the token is not owned by the authenticated user.", http.StatusBadRequest}
	ErrNotImplemented                                 = S3Error{"NotImplemented", "A provided header implies functionality that is not implemented.", http.StatusNotImplemented}
	ErrNotModified                                    = S3Error{"NotModified", "The resource was not changed.", http.StatusNotModified}
	ErrNotSignedUp                                    = S3Error{"NotSignedUp", "Your account is not signed up for Amazon S3.", http.StatusForbidden}
	ErrObjectLockConfigurationNotFoundError           = S3Error{"ObjectLockConfigurationNotFoundError", "Object Lock configuration does not exist for this bucket.", http.StatusNotFound}
	ErrOperationAborted                               = S3Error{"OperationAborted", "A conflicting conditional operation is in progress against this resource.", http.StatusConflict}
	ErrOwnershipControlsNotFoundError                 = S3Error{"OwnershipControlsNotFoundError", "Bucket ownership controls were not found.", http.StatusNotFound}
	ErrPermanentRedirect                              = S3Error{"PermanentRedirect", "Use the specified endpoint; send future requests to that endpoint.", http.StatusMovedPermanently}
	ErrPermanentRedirectControlError                  = S3Error{"PermanentRedirectControlError", "Operation must be addressed using the specified endpoint; redirect future requests accordingly.", http.StatusMovedPermanently}
	ErrPreconditionFailed                             = S3Error{"PreconditionFailed", "At least one specified precondition did not hold.", http.StatusPreconditionFailed}
	ErrRedirect                                       = S3Error{"Redirect", "Temporary redirect while DNS is being updated.", http.StatusTemporaryRedirect}
	ErrRequestHeaderSectionTooLarge                   = S3Error{"RequestHeaderSectionTooLarge", "Request header and query parameters exceed the maximum allowed size.", http.StatusBadRequest}
	ErrRequestIsNotMultiPartContent                   = S3Error{"RequestIsNotMultiPartContent", "Bucket POST must be multipart/form-data.", http.StatusPreconditionFailed}
	ErrRequestTimeout                                 = S3Error{"RequestTimeout", "Socket was not read/written within the timeout period.", http.StatusBadRequest}
	ErrRequestTimeTooSkewed                           = S3Error{"RequestTimeTooSkewed", "Request time differs too much from the server's time.", http.StatusForbidden}
	ErrRequestTorrentOfBucketError                    = S3Error{"RequestTorrentOfBucketError", "Requesting the torrent file of a bucket is not permitted.", http.StatusBadRequest}
	ErrResponseInterrupted                            = S3Error{"ResponseInterrupted", "Error reading WriteGetObjectResponse body; returned to original caller.", http.StatusBadRequest}
	ErrRestoreAlreadyInProgress                       = S3Error{"RestoreAlreadyInProgress", "The object restore is already in progress.", http.StatusConflict}
	ErrServerSideEncryptionConfigurationNotFoundError = S3Error{"ServerSideEncryptionConfigurationNotFoundError", "Server-side encryption configuration was not found.", http.StatusBadRequest}
	ErrServiceUnavailable                             = S3Error{"ServiceUnavailable", "Service is unable to handle the request.", http.StatusServiceUnavailable}
	ErrSignatureDoesNotMatch                          = S3Error{"SignatureDoesNotMatch", "Server-calculated signature does not match the provided signature.", http.StatusForbidden}
	ErrSlowDown                                       = S3Error{"SlowDown", "Please reduce your request rate.", http.StatusServiceUnavailable}
	ErrTagPolicyException                             = S3Error{"TagPolicyException", "Tag policy does not allow the specified value for the tag key.", http.StatusBadRequest}
	ErrTemporaryRedirect                              = S3Error{"TemporaryRedirect", "You are being redirected while DNS is being updated.", http.StatusTemporaryRedirect}
	ErrTokenCodeInvalidError                          = S3Error{"TokenCodeInvalidError", "Provided serial number and/or token code is not valid.", http.StatusBadRequest}
	ErrTokenRefreshRequired                           = S3Error{"TokenRefreshRequired", "The provided token must be refreshed.", http.StatusBadRequest}
	ErrTooManyAccessPoints                            = S3Error{"TooManyAccessPoints", "Attempted to create more access points than allowed for the account.", http.StatusBadRequest}
	ErrTooManyBuckets                                 = S3Error{"TooManyBuckets", "Attempted to create more buckets than allowed for the account.", http.StatusBadRequest}
	ErrTooManyMultiRegionAccessPointregionsError      = S3Error{"TooManyMultiRegionAccessPointregionsError", "Creating a Multi-Region Access Point with more Regions than allowed.", http.StatusBadRequest}
	ErrTooManyMultiRegionAccessPoints                 = S3Error{"TooManyMultiRegionAccessPoints", "Attempted to create more Multi-Region Access Points than allowed.", http.StatusBadRequest}
	ErrUnauthorizedAccessError                        = S3Error{"UnauthorizedAccessError", "China Regions only: request made to a bucket without an ICP license.", http.StatusForbidden}
	ErrUnexpectedContent                              = S3Error{"UnexpectedContent", "Request contains unsupported content.", http.StatusBadRequest}
	ErrUnexpectedIPError                              = S3Error{"UnexpectedIPError", "China Regions only: request rejected due to unexpected IP.", http.StatusForbidden}
	ErrUnsupportedArgument                            = S3Error{"UnsupportedArgument", "Request contained an unsupported argument.", http.StatusBadRequest}
	ErrUnsupportedSignature                           = S3Error{"UnsupportedSignature", "Request signed with unsupported STS token version or unsupported signature version.", http.StatusBadRequest}
	ErrUnresolvableGrantByEmailAddress                = S3Error{"UnresolvableGrantByEmailAddress", "The provided email address does not match any account on record.", http.StatusBadRequest}
	ErrUserKeyMustBeSpecified                         = S3Error{"UserKeyMustBeSpecified", "Bucket POST must contain the specified form field (check field order).", http.StatusBadRequest}
	ErrInvalidTag                                     = S3Error{"InvalidTag", "Tag input is invalid (e.g., duplicates, too long, or system tags).", http.StatusBadRequest}
)

// ErrorResponse is the standard XML error response returned by S3.
type ErrorResponse struct {
	XMLName xml.Name `xml:"Error"`

	Code      string `xml:"Code"`
	Message   string `xml:"Message,omitempty"`
	RequestID string `xml:"RequestId,omitempty"`
	HostID    string `xml:"HostId,omitempty"`
}

// writeErrorResponse writes an error response to the ResponseWriter. The
// provided err must not be nil. If err is not an [S3Error], [ErrInternalError]
// is used.
func writeErrorResponse(w http.ResponseWriter, err error) {
	if err == nil {
		panic("WriteErrorResponse called with nil error")
	}

	var s3Err *S3Error
	if inner := new(S3Error); errors.As(err, inner) {
		s3Err = inner
	} else {
		s3Err = &ErrInternalError
	}

	w.WriteHeader(s3Err.HTTPStatus)
	writeXMLResponse(w, ErrorResponse{
		Code:      s3Err.Code,
		Message:   s3Err.Description,
		RequestID: "", // unused right now (AWS uses it for diagnostic purposes)
		HostID:    "", // unused right now (AWS uses it to identify their server)
	})
}
