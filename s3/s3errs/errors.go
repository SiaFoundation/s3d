package s3errs

import (
	"errors"
	"fmt"
	"net/http"
)

// Error implements error and carries the canonical S3 error code,
// a short description, and the HTTP status code returned by S3.
type Error struct {
	Code        string
	Description string
	HTTPStatus  int
}

// Error implements the error interface.
func (e Error) Error() string {
	return fmt.Sprintf("%s (%d): %s", e.Code, e.HTTPStatus, e.Description)
}

// ErrorCode extracts the S3 error code from an error if possible and otherwise
// returns the code for ErrInternalError.
func ErrorCode(err error) string {
	var s3err Error
	if errors.As(err, &s3err) {
		return s3err.Code
	}
	return ErrInternalError.Code
}

// The following errors are taken from the official list of errors here:
// https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
//
// The list should be complete apart for the following errors which were listed
// without a status code:
// ErrInvalidAddressingHeader                        = S3Error{"InvalidAddressingHeader", "You must specify the Anonymous role.", 0}
var (
	ErrAccessControlListNotSupported                  = Error{"AccessControlListNotSupported", "The bucket does not allow ACLs.", http.StatusBadRequest}
	ErrAccessDenied                                   = Error{"AccessDenied", "Access denied.", http.StatusForbidden}
	ErrAccessPointAlreadyOwnedByYou                   = Error{"AccessPointAlreadyOwnedByYou", "An access point with the same name already exists in your account.", http.StatusConflict}
	ErrAccountProblem                                 = Error{"AccountProblem", "There is a problem with your AWS account preventing the operation.", http.StatusForbidden}
	ErrAllAccessDisabled                              = Error{"AllAccessDisabled", "All access to this Amazon S3 resource has been disabled.", http.StatusForbidden}
	ErrAmbiguousGrantByEmailAddress                   = Error{"AmbiguousGrantByEmailAddress", "The provided email address maps to more than one account.", http.StatusBadRequest}
	ErrAuthorizationHeaderMalformed                   = Error{"AuthorizationHeaderMalformed", "The Authorization header is not valid.", http.StatusBadRequest}
	ErrAuthorizationQueryParametersError              = Error{"AuthorizationQueryParametersError", "The authorization query parameters are not valid.", http.StatusBadRequest}
	ErrBadDigest                                      = Error{"BadDigest", "Provided Content-MD5/checksum does not match what the server received.", http.StatusBadRequest}
	ErrBucketAlreadyExists                            = Error{"BucketAlreadyExists", "Requested bucket name is not available; the namespace is shared.", http.StatusConflict}
	ErrBucketAlreadyOwnedByYou                        = Error{"BucketAlreadyOwnedByYou", "Bucket already exists and is owned by you.", http.StatusConflict}
	ErrBucketHasAccessPointsAttached                  = Error{"BucketHasAccessPointsAttached", "Bucket to delete has access points attached; delete them first.", http.StatusBadRequest}
	ErrBucketNotEmpty                                 = Error{"BucketNotEmpty", "Bucket you tried to delete is not empty.", http.StatusConflict}
	ErrClientTokenConflict                            = Error{"ClientTokenConflict", "Multi-Region Access Point idempotency token was already used for a different request.", http.StatusConflict}
	ErrConnectionClosedByRequester                    = Error{"ConnectionClosedByRequester", "Error while reading WriteGetObjectResponse body; returned to original caller.", http.StatusBadRequest}
	ErrConditionalRequestConflict                     = Error{"ConditionalRequestConflict", "A conflicting operation occurred (e.g., during PutObject or MPU).", http.StatusConflict}
	ErrCredentialsNotSupported                        = Error{"CredentialsNotSupported", "This request does not support credentials.", http.StatusBadRequest}
	ErrCrossLocationLoggingProhibited                 = Error{"CrossLocationLoggingProhibited", "Cross-Region logging is not allowed.", http.StatusForbidden}
	ErrDeviceNotActiveError                           = Error{"DeviceNotActiveError", "The device is not currently active.", http.StatusBadRequest}
	ErrEndpointNotFound                               = Error{"EndpointNotFound", "Direct requests to the correct endpoint.", http.StatusBadRequest}
	ErrEntityTooSmall                                 = Error{"EntityTooSmall", "Proposed upload is smaller than the minimum object size.", http.StatusBadRequest}
	ErrEntityTooLarge                                 = Error{"EntityTooLarge", "Proposed upload exceeds the maximum allowed object size.", http.StatusBadRequest}
	ErrExpiredToken                                   = Error{"ExpiredToken", "The provided token has expired.", http.StatusBadRequest}
	ErrIllegalLocationConstraintException             = Error{"IllegalLocationConstraintException", "Bucket Region mismatch or wrong endpoint for the location constraint.", http.StatusBadRequest}
	ErrIllegalVersioningConfigurationException        = Error{"IllegalVersioningConfigurationException", "The specified versioning configuration is not valid.", http.StatusBadRequest}
	ErrIncompleteBody                                 = Error{"IncompleteBody", "Bytes sent do not match the Content-Length header.", http.StatusBadRequest}
	ErrIncorrectEndpoint                              = Error{"IncorrectEndpoint", "Bucket exists in another Region; use the correct endpoint.", http.StatusBadRequest}
	ErrIncorrectNumberOfFilesInPostRequest            = Error{"IncorrectNumberOfFilesInPostRequest", "POST requires exactly one file upload per request.", http.StatusBadRequest}
	ErrInlineDataTooLarge                             = Error{"InlineDataTooLarge", "Inline data exceeds the maximum allowed size.", http.StatusBadRequest}
	ErrInternalError                                  = Error{"InternalError", "An internal error occurred; try again.", http.StatusInternalServerError}
	ErrInvalidAccessKeyId                             = Error{"InvalidAccessKeyId", "Provided AWS access key ID does not exist in records.", http.StatusForbidden}
	ErrInvalidAccessPoint                             = Error{"InvalidAccessPoint", "Specified access point name or account is not valid.", http.StatusBadRequest}
	ErrInvalidAccessPointAliasError                   = Error{"InvalidAccessPointAliasError", "Specified access point alias name is not valid.", http.StatusBadRequest}
	ErrInvalidArgument                                = Error{"InvalidArgument", "Argument/header invalid, missing, malformed, or too short (≥3 required).", http.StatusBadRequest}
	ErrInvalidBucketAclWithObjectOwnership            = Error{"InvalidBucketAclWithObjectOwnership", "Bucket ACLs cannot be set when ObjectOwnership=BucketOwnerEnforced.", http.StatusBadRequest}
	ErrInvalidBucketName                              = Error{"InvalidBucketName", "The specified bucket name is not valid.", http.StatusBadRequest}
	ErrInvalidBucketOwnerAWSAccountID                 = Error{"InvalidBucketOwnerAWSAccountID", "Expected bucket owner must be an AWS account ID.", http.StatusBadRequest}
	ErrInvalidBucketState                             = Error{"InvalidBucketState", "Request is not valid for the bucket’s current state.", http.StatusConflict}
	ErrInvalidDigest                                  = Error{"InvalidDigest", "Provided Content-MD5/checksum is not valid.", http.StatusBadRequest}
	ErrInvalidEncryptionAlgorithmError                = Error{"InvalidEncryptionAlgorithmError", "Encryption request is not valid; valid value is AES256.", http.StatusBadRequest}
	ErrInvalidHostHeader                              = Error{"InvalidHostHeader", "Host headers used the incorrect addressing style.", http.StatusBadRequest}
	ErrInvalidHttpMethod                              = Error{"InvalidHttpMethod", "Request used an unexpected HTTP method.", http.StatusBadRequest}
	ErrInvalidLocationConstraint                      = Error{"InvalidLocationConstraint", "The specified Region constraint is not valid.", http.StatusBadRequest}
	ErrInvalidObjectState                             = Error{"InvalidObjectState", "Operation is not valid for the object's current state.", http.StatusForbidden}
	ErrInvalidPart                                    = Error{"InvalidPart", "One or more parts not found or ETag did not match.", http.StatusBadRequest}
	ErrInvalidPartOrder                               = Error{"InvalidPartOrder", "Parts list is not in ascending part-number order.", http.StatusBadRequest}
	ErrInvalidPayer                                   = Error{"InvalidPayer", "All access to this object has been disabled.", http.StatusForbidden}
	ErrInvalidPolicyDocument                          = Error{"InvalidPolicyDocument", "Form content does not meet the policy document conditions.", http.StatusBadRequest}
	ErrInvalidRange                                   = Error{"InvalidRange", "Requested byte range cannot be satisfied.", http.StatusRequestedRangeNotSatisfiable}
	ErrInvalidRegion                                  = Error{"InvalidRegion", "Attempted to create a Multi-Region Access Point in a Region you haven't opted in to.", http.StatusForbidden}
	ErrInvalidRequest                                 = Error{"InvalidRequest", "The request is invalid (see docs for common causes including signature version, pagination, lifecycle, acceleration, conflicts, etc.).", http.StatusBadRequest}
	ErrInvalidSessionException                        = Error{"InvalidSessionException", "Session no longer exists because it timed out or expired.", http.StatusBadRequest}
	ErrInvalidSignature                               = Error{"InvalidSignature", "Server-calculated signature does not match the provided signature.", http.StatusBadRequest}
	ErrInvalidSecurity                                = Error{"InvalidSecurity", "Provided security credentials are not valid.", http.StatusForbidden}
	ErrInvalidSOAPRequest                             = Error{"InvalidSOAPRequest", "SOAP request body is not valid.", http.StatusBadRequest}
	ErrInvalidStorageClass                            = Error{"InvalidStorageClass", "The specified storage class is not valid.", http.StatusBadRequest}
	ErrInvalidTargetBucketForLogging                  = Error{"InvalidTargetBucketForLogging", "Logging target bucket missing, not owned by you, or lacks required grants.", http.StatusBadRequest}
	ErrInvalidToken                                   = Error{"InvalidToken", "Provided token is malformed or otherwise invalid.", http.StatusBadRequest}
	ErrInvalidURI                                     = Error{"InvalidURI", "Specified URI could not be parsed.", http.StatusBadRequest}
	ErrKeyTooLongError                                = Error{"KeyTooLongError", "The object key is too long.", http.StatusBadRequest}
	ErrKMSDisabledException                           = Error{"KMS.DisabledException", "Specified KMS key is disabled.", http.StatusBadRequest}
	ErrKMSInvalidKeyUsageException                    = Error{"KMS.InvalidKeyUsageException", "KMS KeyUsage or algorithm is incompatible with the operation/key type.", http.StatusBadRequest}
	ErrKMSInvalidStateException                       = Error{"KMS.KMSInvalidStateException", "Resource/key state not valid for the request.", http.StatusBadRequest}
	ErrKMSNotFoundException                           = Error{"KMS.NotFoundException", "Specified KMS entity or resource was not found.", http.StatusBadRequest}
	ErrMalformedACLError                              = Error{"MalformedACLError", "Provided ACL is not well-formed or fails schema validation.", http.StatusBadRequest}
	ErrMalformedPOSTRequest                           = Error{"MalformedPOSTRequest", "POST body is not well-formed multipart/form-data.", http.StatusBadRequest}
	ErrMalformedXML                                   = Error{"MalformedXML", "Provided XML is not well-formed or fails schema validation.", http.StatusBadRequest}
	ErrMalformedPolicy                                = Error{"MalformedPolicy", "Your policy contains a principal that is not valid.", http.StatusBadRequest}
	ErrMaxMessageLengthExceeded                       = Error{"MaxMessageLengthExceeded", "Request was too large.", http.StatusBadRequest}
	ErrMaxPostPreDataLengthExceededError              = Error{"MaxPostPreDataLengthExceededError", "POST fields preceding the file were too large.", http.StatusBadRequest}
	ErrMetadataTooLarge                               = Error{"MetadataTooLarge", "Metadata headers exceed the maximum allowed size.", http.StatusBadRequest}
	ErrMethodNotAllowed                               = Error{"MethodNotAllowed", "The specified method is not allowed for this resource.", http.StatusMethodNotAllowed}
	ErrMissingAttachment                              = Error{"MissingAttachment", "A SOAP attachment was expected but not found.", http.StatusBadRequest}
	ErrMissingAuthenticationToken                     = Error{"MissingAuthenticationToken", "The request was not signed.", http.StatusForbidden}
	ErrMissingContentLength                           = Error{"MissingContentLength", "Content-Length header is required.", http.StatusLengthRequired}
	ErrMissingRequestBodyError                        = Error{"MissingRequestBodyError", "An empty XML document was sent as the request.", http.StatusBadRequest}
	ErrMissingSecurityElement                         = Error{"MissingSecurityElement", "SOAP 1.1 request is missing a security element.", http.StatusBadRequest}
	ErrMissingSecurityHeader                          = Error{"MissingSecurityHeader", "Request is missing a required header.", http.StatusBadRequest}
	ErrNoLoggingStatusForKey                          = Error{"NoLoggingStatusForKey", "No logging status subresource exists for a key.", http.StatusBadRequest}
	ErrNoSuchAccessPoint                              = Error{"NoSuchAccessPoint", "The specified access point does not exist.", http.StatusNotFound}
	ErrNoSuchAsyncRequest                             = Error{"NoSuchAsyncRequest", "The specified request was not found.", http.StatusNotFound}
	ErrNoSuchBucket                                   = Error{"NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound}
	ErrNoSuchBucketPolicy                             = Error{"NoSuchBucketPolicy", "The specified bucket does not have a bucket policy.", http.StatusNotFound}
	ErrNoSuchCORSConfiguration                        = Error{"NoSuchCORSConfiguration", "The specified bucket does not have a CORS configuration.", http.StatusNotFound}
	ErrNoSuchKey                                      = Error{"NoSuchKey", "The specified key does not exist.", http.StatusNotFound}
	ErrNoSuchLifecycleConfiguration                   = Error{"NoSuchLifecycleConfiguration", "The specified lifecycle configuration does not exist.", http.StatusNotFound}
	ErrNoSuchMultiRegionAccessPoint                   = Error{"NoSuchMultiRegionAccessPoint", "The specified Multi-Region Access Point does not exist.", http.StatusNotFound}
	ErrNoSuchObjectLockConfiguration                  = Error{"NoSuchObjectLockConfiguration", "The specified object does not have an Object Lock configuration.", http.StatusNotFound}
	ErrNoSuchTagSet                                   = Error{"NoSuchTagSet", "The specified tag does not exist.", http.StatusNotFound}
	ErrNoSuchUpload                                   = Error{"NoSuchUpload", "Specified multipart upload does not exist (invalid ID, aborted, or completed).", http.StatusNotFound}
	ErrNoSuchVersion                                  = Error{"NoSuchVersion", "Specified version ID does not match an existing version.", http.StatusNotFound}
	ErrNoSuchWebsiteConfiguration                     = Error{"NoSuchWebsiteConfiguration", "The specified bucket does not have a website configuration.", http.StatusNotFound}
	ErrNoTransformationDefined                        = Error{"NoTransformationDefined", "No transformation found for this Object Lambda Access Point.", http.StatusNotFound}
	ErrNotDeviceOwnerError                            = Error{"NotDeviceOwnerError", "The device that generated the token is not owned by the authenticated user.", http.StatusBadRequest}
	ErrNotImplemented                                 = Error{"NotImplemented", "A provided header implies functionality that is not implemented.", http.StatusNotImplemented}
	ErrNotModified                                    = Error{"NotModified", "The resource was not changed.", http.StatusNotModified}
	ErrNotSignedUp                                    = Error{"NotSignedUp", "Your account is not signed up for Amazon S3.", http.StatusForbidden}
	ErrObjectLockConfigurationNotFoundError           = Error{"ObjectLockConfigurationNotFoundError", "Object Lock configuration does not exist for this bucket.", http.StatusNotFound}
	ErrOperationAborted                               = Error{"OperationAborted", "A conflicting conditional operation is in progress against this resource.", http.StatusConflict}
	ErrOwnershipControlsNotFoundError                 = Error{"OwnershipControlsNotFoundError", "Bucket ownership controls were not found.", http.StatusNotFound}
	ErrPermanentRedirect                              = Error{"PermanentRedirect", "Use the specified endpoint; send future requests to that endpoint.", http.StatusMovedPermanently}
	ErrPermanentRedirectControlError                  = Error{"PermanentRedirectControlError", "Operation must be addressed using the specified endpoint; redirect future requests accordingly.", http.StatusMovedPermanently}
	ErrPreconditionFailed                             = Error{"PreconditionFailed", "At least one specified precondition did not hold.", http.StatusPreconditionFailed}
	ErrRedirect                                       = Error{"Redirect", "Temporary redirect while DNS is being updated.", http.StatusTemporaryRedirect}
	ErrRequestHeaderSectionTooLarge                   = Error{"RequestHeaderSectionTooLarge", "Request header and query parameters exceed the maximum allowed size.", http.StatusBadRequest}
	ErrRequestIsNotMultiPartContent                   = Error{"RequestIsNotMultiPartContent", "Bucket POST must be multipart/form-data.", http.StatusPreconditionFailed}
	ErrRequestTimeout                                 = Error{"RequestTimeout", "Socket was not read/written within the timeout period.", http.StatusBadRequest}
	ErrRequestTimeTooSkewed                           = Error{"RequestTimeTooSkewed", "Request time differs too much from the server's time.", http.StatusForbidden}
	ErrRequestTorrentOfBucketError                    = Error{"RequestTorrentOfBucketError", "Requesting the torrent file of a bucket is not permitted.", http.StatusBadRequest}
	ErrResponseInterrupted                            = Error{"ResponseInterrupted", "Error reading WriteGetObjectResponse body; returned to original caller.", http.StatusBadRequest}
	ErrRestoreAlreadyInProgress                       = Error{"RestoreAlreadyInProgress", "The object restore is already in progress.", http.StatusConflict}
	ErrServerSideEncryptionConfigurationNotFoundError = Error{"ServerSideEncryptionConfigurationNotFoundError", "Server-side encryption configuration was not found.", http.StatusBadRequest}
	ErrServiceUnavailable                             = Error{"ServiceUnavailable", "Service is unable to handle the request.", http.StatusServiceUnavailable}
	ErrSignatureDoesNotMatch                          = Error{"SignatureDoesNotMatch", "Server-calculated signature does not match the provided signature.", http.StatusForbidden}
	ErrSlowDown                                       = Error{"SlowDown", "Please reduce your request rate.", http.StatusServiceUnavailable}
	ErrStorageLimitExceeded                           = Error{"StorageLimitExceeded", "The local disk usage limit has been reached. Try again after pending data has been offloaded.", http.StatusServiceUnavailable}
	ErrTagPolicyException                             = Error{"TagPolicyException", "Tag policy does not allow the specified value for the tag key.", http.StatusBadRequest}
	ErrTemporaryRedirect                              = Error{"TemporaryRedirect", "You are being redirected while DNS is being updated.", http.StatusTemporaryRedirect}
	ErrTokenCodeInvalidError                          = Error{"TokenCodeInvalidError", "Provided serial number and/or token code is not valid.", http.StatusBadRequest}
	ErrTokenRefreshRequired                           = Error{"TokenRefreshRequired", "The provided token must be refreshed.", http.StatusBadRequest}
	ErrTooManyAccessPoints                            = Error{"TooManyAccessPoints", "Attempted to create more access points than allowed for the account.", http.StatusBadRequest}
	ErrTooManyBuckets                                 = Error{"TooManyBuckets", "Attempted to create more buckets than allowed for the account.", http.StatusBadRequest}
	ErrTooManyMultiRegionAccessPointregionsError      = Error{"TooManyMultiRegionAccessPointregionsError", "Creating a Multi-Region Access Point with more Regions than allowed.", http.StatusBadRequest}
	ErrTooManyMultiRegionAccessPoints                 = Error{"TooManyMultiRegionAccessPoints", "Attempted to create more Multi-Region Access Points than allowed.", http.StatusBadRequest}
	ErrUnauthorizedAccessError                        = Error{"UnauthorizedAccessError", "China Regions only: request made to a bucket without an ICP license.", http.StatusForbidden}
	ErrUnexpectedContent                              = Error{"UnexpectedContent", "Request contains unsupported content.", http.StatusBadRequest}
	ErrUnexpectedIPError                              = Error{"UnexpectedIPError", "China Regions only: request rejected due to unexpected IP.", http.StatusForbidden}
	ErrUnsupportedArgument                            = Error{"UnsupportedArgument", "Request contained an unsupported argument.", http.StatusBadRequest}
	ErrUnsupportedSignature                           = Error{"UnsupportedSignature", "Request signed with unsupported STS token version or unsupported signature version.", http.StatusBadRequest}
	ErrUnresolvableGrantByEmailAddress                = Error{"UnresolvableGrantByEmailAddress", "The provided email address does not match any account on record.", http.StatusBadRequest}
	ErrUserKeyMustBeSpecified                         = Error{"UserKeyMustBeSpecified", "Bucket POST must contain the specified form field (check field order).", http.StatusBadRequest}
	ErrInvalidTag                                     = Error{"InvalidTag", "Tag input is invalid (e.g., duplicates, too long, or system tags).", http.StatusBadRequest}
)
