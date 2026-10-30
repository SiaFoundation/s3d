package s3

import (
	"crypto/tls"
	"maps"
	"math"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3/auth"
)

// AnonymousUserID is the aws:userid and aws:PrincipalAccount of anonymous
// requests in bucket policy conditions, as in S3. No user may have this name.
const AnonymousUserID = "anonymous"

// policyContext maps lowercase condition keys, which are case insensitive, to
// their case-sensitive values.
type policyContext map[string][]string

// trustedProxy reports whether ip is one of the trusted proxies.
func (s *s3) trustedProxy(ip netip.Addr) bool {
	for _, prefix := range s.trustedProxies {
		if prefix.Contains(ip.Unmap()) {
			return true
		}
	}
	return false
}

// requestPolicyContext returns the condition keys every action shares. Those
// specific to an action are added by [bucketAccess.actionPolicyContext].
func (s *s3) requestPolicyContext(r *http.Request, caller *auth.Caller) policyContext {
	now := time.Now().UTC()
	// a signed request names its region; an anonymous one gets the server's
	region := s.region
	if caller != nil {
		region = caller.Region
	} else if region == "" {
		region = DefaultRegion
	}
	requestContext := policyContext{
		"aws:requestedregion":  {region},
		"aws:currenttime":      {now.Format(time.RFC3339Nano)},
		"aws:epochtime":        {strconv.FormatInt(now.Unix(), 10)},
		"aws:securetransport":  {strconv.FormatBool(r.TLS != nil)},
		"aws:principaltype":    {"Anonymous"},
		"aws:userid":           {AnonymousUserID},
		"aws:principalaccount": {AnonymousUserID},
		// s3d is never called through another AWS service
		"aws:viaawsservice": {"false"},
	}
	ip, err := parseHostAddr(r.RemoteAddr)
	proxied := err == nil && s.trustedProxy(ip)
	if proxied {
		// a list may be split across several header lines
		if forwarded := r.Header.Values("X-Forwarded-For"); len(forwarded) > 0 {
			chain := strings.Split(strings.Join(forwarded, ","), ",")
			for i := len(chain) - 1; i >= 0 && ip.IsValid() && s.trustedProxy(ip); i-- {
				// an unparsable hop leaves the client unknown rather than
				// attributing the request to the proxy
				ip, _ = parseHostAddr(chain[i])
			}
		}
		// the immediate trusted proxy must overwrite X-Forwarded-Proto
		if proto := r.Header.Values("X-Forwarded-Proto"); len(proto) == 1 {
			switch proto[0] {
			case "https":
				requestContext["aws:securetransport"] = []string{"true"}
			case "http":
				requestContext["aws:securetransport"] = []string{"false"}
			}
		}
	}
	if ip.IsValid() {
		requestContext["aws:sourceip"] = []string{ip.Unmap().String()}
	}
	// a proxied connection's TLS version is the proxy's, not the client's
	if r.TLS != nil && !proxied {
		if version, ok := strings.CutPrefix(tls.VersionName(r.TLS.Version), "TLS "); ok {
			requestContext["s3:tlsversion"] = []string{version}
		}
	}
	if referer := r.Referer(); referer != "" {
		requestContext["aws:referer"] = []string{referer}
	}
	if userAgent := r.UserAgent(); userAgent != "" {
		requestContext["aws:useragent"] = []string{userAgent}
	}
	if hash, ok := r.Context().Value(contentSHA256Key{}).(string); ok {
		requestContext["s3:x-amz-content-sha256"] = []string{hash}
	}
	if caller != nil {
		// a user is presented as the root user of an AWS account with the
		// user's ID, which has no aws:username
		requestContext["aws:userid"] = []string{caller.UserID}
		requestContext["aws:principalaccount"] = []string{caller.UserID}
		requestContext["aws:principalarn"] = []string{"arn:aws:iam::" + caller.UserID + ":root"}
		requestContext["aws:principaltype"] = []string{"Account"}
		requestContext["aws:principalisawsservice"] = []string{"false"}
		requestContext["s3:signatureversion"] = []string{auth.AuthorizationAWS4HMACSHA256}
		if caller.Presigned {
			requestContext["s3:authtype"] = []string{"REST-QUERY-STRING"}
			// S3 only reports the signature age of presigned requests
			age := max(int64(0), now.Sub(caller.SignedAt).Milliseconds())
			requestContext["s3:signatureage"] = []string{strconv.FormatInt(age, 10)}
		} else {
			requestContext["s3:authtype"] = []string{"REST-HEADER"}
		}
	}
	return requestContext
}

// contentSHA256Key is the request context key of the X-Amz-Content-Sha256
// header as the client sent it, since authentication normalizes the header.
type contentSHA256Key struct{}

// parseHostAddr parses a remote address or X-Forwarded-For hop, either of
// which may carry a port.
func parseHostAddr(host string) (netip.Addr, error) {
	host = strings.TrimSpace(host)
	if addrPort, err := netip.ParseAddrPort(host); err == nil {
		return addrPort.Addr(), nil
	}
	return netip.ParseAddr(strings.TrimSuffix(strings.TrimPrefix(host, "["), "]"))
}

// putObjectConditionHeaders are the PutObject headers s3d honors that S3
// exposes as "s3:<header>" condition keys. The keys of ignored headers are
// missing.
var putObjectConditionHeaders = []string{"If-Match", "If-None-Match", "X-Amz-Metadata-Directive"}

// actionPolicyContext adds the action-specific keys the request supplies. The
// handlers of copies and multi-delete requests add their own version IDs and
// ETags.
func (a *bucketAccess) actionPolicyContext(action string) (policyContext, error) {
	if a.requestContext == nil {
		return nil, nil
	}
	extra := make(policyContext)
	query := a.request.URL.Query()
	switch action {
	case actionListBucket, actionListBucketVersions:
		if query.Has("prefix") {
			extra["s3:prefix"] = []string{query.Get("prefix")}
		}
		if query.Has("delimiter") {
			extra["s3:delimiter"] = []string{query.Get("delimiter")}
		}
		// a listing always has a limit; HeadBucket is not a listing
		if a.request.Method == http.MethodGet {
			maxKeys, err := parseClampedInt(query.Get("max-keys"), DefaultMaxBucketKeys, 0, MaxBucketKeys)
			if err != nil {
				return nil, err
			}
			extra["s3:max-keys"] = []string{strconv.FormatInt(maxKeys, 10)}
		}
	case actionGetObjectVersion, actionDeleteObjectVersion:
		if version := VersionFromQuery(query["versionId"]); version.Specified {
			extra["s3:versionid"] = []string{FormatVersion(version.ID)}
		}
	case actionDeleteObject:
		// S3 supplies s3:if-match to DeleteObject but not DeleteObjectVersion
		if value := a.request.Header.Get("If-Match"); value != "" {
			extra["s3:if-match"] = []string{value}
		}
	case actionPutObject:
		// a POST Object's handler supplies its form's tags. Invalid tags are
		// left to the handler.
		if a.headerTagConditions {
			if tags, err := requestTags(a.request.Header); err == nil {
				maps.Copy(extra, requestTagPolicyContext(tags))
			}
		}
		// only CopyObject and UploadPartCopy, both PUTs, have a copy source
		if source := a.request.Header.Get("X-Amz-Copy-Source"); source != "" && a.request.Method == http.MethodPut {
			if bucket, object, _, err := parseSource(source); err == nil {
				extra["s3:x-amz-copy-source"] = []string{bucket + "/" + object}
			}
		}
		for _, header := range putObjectConditionHeaders {
			if value := a.request.Header.Get(header); value != "" {
				extra["s3:"+strings.ToLower(header)] = []string{value}
			}
		}
		// false for CreateMultipartUpload, UploadPart and UploadPartCopy,
		// which take no conditional headers
		creates := !query.Has("uploads")
		uploadID := query.Get("uploadId")
		if uploadID != "" {
			creates = a.request.Method == http.MethodPost // CompleteMultipartUpload
		}
		extra["s3:objectcreationoperation"] = []string{strconv.FormatBool(creates)}
		// an upload's lock is set when it is created, so parts and completions
		// ignore lock headers. An invalid lock is rejected by the handler.
		if lock, err := requestObjectLock(a.request.Header); err == nil && uploadID == "" {
			maps.Copy(extra, objectLockPolicyContext(lock))
		}
	}
	return a.requestContext.merge(extra), nil
}

// objectLockPolicyContext returns the condition keys of a lock the request
// applies.
func objectLockPolicyContext(lock *ObjectLockState) policyContext {
	extra := make(policyContext)
	if lock.Retained() {
		// a partial day counts as a whole one
		days := int64(math.Ceil(time.Until(lock.RetainUntil).Hours() / 24))
		extra["s3:object-lock-mode"] = []string{lock.Mode}
		extra["s3:object-lock-retain-until-date"] = []string{lock.RetainUntil.UTC().Format(retainUntilLayout)}
		extra["s3:object-lock-remaining-retention-days"] = []string{strconv.FormatInt(days, 10)}
	}
	if lock != nil && lock.LegalHold != "" {
		extra["s3:object-lock-legal-hold"] = []string{lock.LegalHold}
	}
	return extra
}

// The condition key prefixes of a tag the request applies and of a tag already
// on the version, each followed by the lowercase tag key.
const (
	requestObjectTagPrefix  = "s3:requestobjecttag/"
	existingObjectTagPrefix = "s3:existingobjecttag/"
)

// requestTagPolicyContext returns the condition keys of the tags the request
// applies.
func requestTagPolicyContext(tags TagSet) policyContext {
	extra := make(policyContext)
	for _, tag := range tags {
		key := requestObjectTagPrefix + strings.ToLower(tag.Key)
		extra[key] = append(extra[key], tag.Value)
		extra["s3:requestobjecttagkeys"] = append(extra["s3:requestobjecttagkeys"], tag.Key)
	}
	return extra
}

// existingTagPolicyContext returns the condition keys of the existing tags of
// the version a request acts on.
func existingTagPolicyContext(tags TagSet) policyContext {
	if len(tags) == 0 {
		return nil
	}
	extra := make(policyContext)
	for _, tag := range tags {
		key := existingObjectTagPrefix + strings.ToLower(tag.Key)
		extra[key] = append(extra[key], tag.Value)
	}
	return extra
}

// merge returns c with extra's keys added, copying c only if needed.
func (c policyContext) merge(extra policyContext) policyContext {
	if len(extra) == 0 {
		return c
	}
	merged := make(policyContext, len(c)+len(extra))
	maps.Copy(merged, c)
	maps.Copy(merged, extra)
	return merged
}

// validConditionKeys are the AWS global and S3 condition keys, lowercase. S3
// rejects any other key.
//
// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-keys.html
// https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html
var validConditionKeys = map[string]bool{
	"aws:assumedroot":                                    true,
	"aws:calledvia":                                      true,
	"aws:calledviafirst":                                 true,
	"aws:calledvialast":                                  true,
	"aws:currenttime":                                    true,
	"aws:ec2instancesourceprivateipv4":                   true,
	"aws:ec2instancesourcevpc":                           true,
	"aws:epochtime":                                      true,
	"aws:federatedprovider":                              true,
	"aws:multifactorauthage":                             true,
	"aws:multifactorauthpresent":                         true,
	"aws:principalaccount":                               true,
	"aws:principalarn":                                   true,
	"aws:principalisawsservice":                          true,
	"aws:principalorgid":                                 true,
	"aws:principalorgpaths":                              true,
	"aws:principalservicename":                           true,
	"aws:principalservicenameslist":                      true,
	"aws:principaltype":                                  true,
	"aws:referer":                                        true,
	"aws:requestedregion":                                true,
	"aws:resourceaccount":                                true,
	"aws:resourceorgid":                                  true,
	"aws:resourceorgpaths":                               true,
	"aws:securetransport":                                true,
	"aws:sourceaccount":                                  true,
	"aws:sourcearn":                                      true,
	"aws:sourceidentity":                                 true,
	"aws:sourceip":                                       true,
	"aws:sourceorgid":                                    true,
	"aws:sourceorgpaths":                                 true,
	"aws:sourceowner":                                    true,
	"aws:sourcevpc":                                      true,
	"aws:sourcevpcarn":                                   true,
	"aws:sourcevpce":                                     true,
	"aws:tagkeys":                                        true,
	"aws:tokenissuetime":                                 true,
	"aws:useragent":                                      true,
	"aws:userid":                                         true,
	"aws:username":                                       true,
	"aws:viaawsservice":                                  true,
	"aws:vpceaccount":                                    true,
	"aws:vpceorgid":                                      true,
	"aws:vpceorgpaths":                                   true,
	"aws:vpcsourceip":                                    true,
	"s3:accessgrantscope":                                true,
	"s3:accessgrantsinstancearn":                         true,
	"s3:accessgrantslocationscope":                       true,
	"s3:accesspointnetworkorigin":                        true,
	"s3:annotation-prefix":                               true,
	"s3:authtype":                                        true,
	"s3:dataaccesspointaccount":                          true,
	"s3:dataaccesspointarn":                              true,
	"s3:delimiter":                                       true,
	"s3:deliverysourcearn":                               true,
	"s3:destinationregion":                               true,
	"s3:existingjoboperation":                            true,
	"s3:existingjobpriority":                             true,
	"s3:if-match":                                        true,
	"s3:if-none-match":                                   true,
	"s3:inventoryaccessibleoptionalfields":               true,
	"s3:isreplicationpauserequest":                       true,
	"s3:jobsuspendedcause":                               true,
	"s3:locationconstraint":                              true,
	"s3:logtype":                                         true,
	"s3:max-annotation-results":                          true,
	"s3:max-keys":                                        true,
	"s3:object-lock-event-hold":                          true,
	"s3:object-lock-event-hold-duration-days":            true,
	"s3:object-lock-legal-hold":                          true,
	"s3:object-lock-mode":                                true,
	"s3:object-lock-remaining-retention-days":            true,
	"s3:object-lock-retain-until-date":                   true,
	"s3:objectcreationoperation":                         true,
	"s3:prefix":                                          true,
	"s3:requestjoboperation":                             true,
	"s3:requestjobpriority":                              true,
	"s3:requestobjecttagkeys":                            true,
	"s3:resourceaccount":                                 true,
	"s3:resourcearnbeingauthorized":                      true,
	"s3:signatureage":                                    true,
	"s3:signatureversion":                                true,
	"s3:tlsversion":                                      true,
	"s3:versionid":                                       true,
	"s3:x-amz-acl":                                       true,
	"s3:x-amz-bucket-namespace":                          true,
	"s3:x-amz-content-sha256":                            true,
	"s3:x-amz-copy-source":                               true,
	"s3:x-amz-grant-full-control":                        true,
	"s3:x-amz-grant-read":                                true,
	"s3:x-amz-grant-read-acp":                            true,
	"s3:x-amz-grant-write":                               true,
	"s3:x-amz-grant-write-acp":                           true,
	"s3:x-amz-metadata-directive":                        true,
	"s3:x-amz-object-annotation-directive":               true,
	"s3:x-amz-object-if-match":                           true,
	"s3:x-amz-object-ownership":                          true,
	"s3:x-amz-server-side-encryption":                    true,
	"s3:x-amz-server-side-encryption-aws-kms-key-id":     true,
	"s3:x-amz-server-side-encryption-customer-algorithm": true,
	"s3:x-amz-storage-class":                             true,
	"s3:x-amz-website-redirect-location":                 true,
}

// conditionKeyPrefixes are the condition keys that take a tag key suffix.
var conditionKeyPrefixes = []string{
	"aws:principaltag/",
	"aws:requesttag/",
	"aws:resourcetag/",
	"s3:accesspointtag/",
	"s3:buckettag/",
	existingObjectTagPrefix,
	requestObjectTagPrefix,
}

// validConditionKey reports whether key is a known condition key, ignoring
// case. Keys s3d cannot populate are valid and evaluate as missing.
func validConditionKey(key string) bool {
	key = strings.ToLower(key)
	if validConditionKeys[key] {
		return true
	}
	for _, prefix := range conditionKeyPrefixes {
		if len(key) > len(prefix) && strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}
