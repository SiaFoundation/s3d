package s3

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3/auth"
	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
)

// POST Object form field names, lowercased. S3 matches a field name without
// regard to case and the AWS SDKs disagree on how they spell them, so a name is
// lowercased as the form is parsed.
const (
	postFieldAlgorithm  = "x-amz-algorithm"
	postFieldBucket     = "bucket"
	postFieldCredential = "x-amz-credential"
	postFieldDate       = "x-amz-date"
	postFieldFile       = "file"
	postFieldKey        = "key"
	postFieldPolicy     = "policy"
	postFieldRedirect   = "success_action_redirect"
	postFieldSignature  = "x-amz-signature"
	postFieldStatus     = "success_action_status"
	postFieldToken      = "x-amz-security-token"

	// postFieldRedirectLegacy is the name S3 accepted for the redirect before
	// postFieldRedirect replaced it.
	postFieldRedirectLegacy = "redirect"
)

const (
	// maxPostPolicySize is the largest accepted policy document, which S3
	// documents as 20 KB.
	maxPostPolicySize = 20 * 1024

	// maxPostFieldBytes bounds the fields preceding the file, so a form cannot
	// buffer an arbitrary amount before the upload starts.
	maxPostFieldBytes = 64 * 1024

	// postFilenameVariable is replaced in the key with the name the client gave
	// the file it uploaded.
	postFilenameVariable = "${filename}"
)

// postForm holds the fields of a POST Object request that precede the file,
// keyed by their lowercased name, and the name the client gave the file.
type postForm struct {
	fields   map[string]string
	filename string
}

// get returns the value of a field, or "" if the form does not carry it.
func (f postForm) get(name string) string {
	return f.fields[name]
}

// metadata returns the object metadata the form describes. The fields that
// authenticate the request describe the request rather than the object, so they
// are left out.
func (f postForm) metadata() (map[string]string, error) {
	headers := make(map[string][]string, len(f.fields))
	for name, value := range f.fields {
		switch name {
		case postFieldAlgorithm, postFieldCredential, postFieldDate,
			postFieldSignature, postFieldToken:
			continue
		}
		headers[name] = []string{value}
	}
	return metadataHeaders(headers, MetadataSizeLimit)
}

// redirect returns the URL the form wants a successful upload to redirect to.
func (f postForm) redirect() string {
	if target := f.get(postFieldRedirect); target != "" {
		return target
	}
	return f.get(postFieldRedirectLegacy)
}

// parsePostForm reads the fields preceding the file and returns them with the
// file's part still open, so the upload streams rather than being buffered.
func parsePostForm(r *http.Request) (postForm, *multipart.Part, error) {
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/form-data" {
		return postForm{}, nil, s3errs.ErrRequestIsNotMultiPartContent
	} else if params["boundary"] == "" {
		return postForm{}, nil, s3errs.ErrMalformedPOSTRequest
	}

	form := postForm{fields: make(map[string]string)}
	reader := multipart.NewReader(r.Body, params["boundary"])
	var buffered int
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			// the file is required and every field has to precede it
			return postForm{}, nil, s3errs.ErrIncorrectNumberOfFilesInPostRequest
		} else if err != nil {
			return postForm{}, nil, s3errs.ErrMalformedPOSTRequest
		}

		name := strings.ToLower(part.FormName())
		if name == postFieldFile {
			form.filename = part.FileName()
			return form, part, nil
		}

		value, err := io.ReadAll(io.LimitReader(part, int64(maxPostFieldBytes-buffered)+1))
		part.Close()
		if err != nil {
			return postForm{}, nil, s3errs.ErrMalformedPOSTRequest
		}
		buffered += len(value)
		if buffered > maxPostFieldBytes {
			return postForm{}, nil, s3errs.ErrMaxPostPreDataLengthExceededError
		}
		form.fields[name] = string(value)
	}
}

// postPolicy is the decoded policy document of a POST Object request.
type postPolicy struct {
	expiration time.Time
	matches    []postMatch

	// minSize and maxSize are the bounds of a content-length-range condition.
	// maxSize is negative when the policy carries no such condition.
	minSize int64
	maxSize int64
}

// postMatch is one field condition of a policy document. The exact and the
// prefix form differ only in how the value is compared.
type postMatch struct {
	field      string
	value      string
	startsWith bool
}

// parsePostPolicy decodes the base64 policy document a form submitted.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html
func parsePostPolicy(encoded string) (postPolicy, error) {
	if len(encoded) > maxPostPolicySize {
		return postPolicy{}, s3errs.ErrPolicyTooLarge
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return postPolicy{}, s3errs.ErrMalformedPolicy
	}

	var document struct {
		Expiration string            `json:"expiration"`
		Conditions []json.RawMessage `json:"conditions"`
	}
	if err := json.Unmarshal(decoded, &document); err != nil {
		return postPolicy{}, s3errs.ErrMalformedPolicy
	}

	policy := postPolicy{maxSize: -1}
	if policy.expiration, err = time.Parse(time.RFC3339, document.Expiration); err != nil {
		return postPolicy{}, s3errs.ErrMalformedPolicy
	}
	for _, condition := range document.Conditions {
		if err := policy.addCondition(condition); err != nil {
			return postPolicy{}, err
		}
	}
	return policy, nil
}

// addCondition adds one condition of a policy document, which is either an
// object naming a single field or a three element array naming an operator.
func (p *postPolicy) addCondition(condition json.RawMessage) error {
	var exact map[string]string
	if err := json.Unmarshal(condition, &exact); err == nil {
		for field, value := range exact {
			p.matches = append(p.matches, postMatch{
				field: strings.ToLower(field),
				value: value,
			})
		}
		return nil
	}

	// the operator form mixes strings and numbers, so its elements are decoded
	// one at a time
	var elements []json.RawMessage
	var operator string
	if err := json.Unmarshal(condition, &elements); err != nil || len(elements) != 3 {
		return s3errs.ErrMalformedPolicy
	} else if err := json.Unmarshal(elements[0], &operator); err != nil {
		return s3errs.ErrMalformedPolicy
	}

	switch strings.ToLower(operator) {
	case "eq", "starts-with":
		var field, value string
		if err := json.Unmarshal(elements[1], &field); err != nil {
			return s3errs.ErrMalformedPolicy
		} else if err := json.Unmarshal(elements[2], &value); err != nil {
			return s3errs.ErrMalformedPolicy
		} else if !strings.HasPrefix(field, "$") {
			return s3errs.ErrMalformedPolicy
		}
		p.matches = append(p.matches, postMatch{
			field:      strings.ToLower(strings.TrimPrefix(field, "$")),
			value:      value,
			startsWith: operator == "starts-with",
		})
	case "content-length-range":
		minSize, err := parsePostSize(elements[1])
		if err != nil {
			return err
		}
		maxSize, err := parsePostSize(elements[2])
		if err != nil {
			return err
		} else if minSize < 0 || maxSize < minSize {
			return s3errs.ErrMalformedPolicy
		}
		p.minSize, p.maxSize = minSize, maxSize
	default:
		return s3errs.ErrMalformedPolicy
	}
	return nil
}

// parsePostSize decodes a content-length-range bound, which S3 accepts as
// either a number or a string holding one.
func parsePostSize(element json.RawMessage) (int64, error) {
	var number json.Number
	if err := json.Unmarshal(element, &number); err != nil {
		var text string
		if err := json.Unmarshal(element, &text); err != nil {
			return 0, s3errs.ErrMalformedPolicy
		}
		number = json.Number(text)
	}
	size, err := number.Int64()
	if err != nil {
		return 0, s3errs.ErrMalformedPolicy
	}
	return size, nil
}

// check verifies the form against the policy. Every condition has to be
// satisfied and every field has to be named by a condition, which is what stops
// a client from adding a field the signer never approved.
func (p postPolicy) check(form postForm, now time.Time) error {
	if now.After(p.expiration) {
		return s3errs.ErrAccessDeniedExpired
	}

	named := make(map[string]struct{}, len(p.matches))
	for _, match := range p.matches {
		// a field the form left out is matched as empty, so a condition that
		// demands a particular value fails while one that demands nothing holds
		value := form.fields[match.field]
		if match.startsWith && !strings.HasPrefix(value, match.value) {
			return s3errs.ErrInvalidPolicyDocument
		} else if !match.startsWith && value != match.value {
			return s3errs.ErrInvalidPolicyDocument
		}
		named[match.field] = struct{}{}
	}
	for field := range form.fields {
		if _, ok := named[field]; ok {
			continue
		}
		// the policy and the signature over it cannot name themselves, and S3
		// reserves the x-ignore- prefix for fields a form carries for its own
		// use
		switch {
		case field == postFieldPolicy, field == postFieldSignature:
		case strings.HasPrefix(field, "x-ignore-"):
		default:
			return s3errs.ErrInvalidPolicyDocument
		}
	}
	return nil
}

// postBody enforces the lower bound of a content-length-range condition, which
// is only known to be violated once the body ends.
type postBody struct {
	reader io.Reader
	min    int64
	read   int64
}

func (b *postBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.read += int64(n)
	if errors.Is(err, io.EOF) && b.read < b.min {
		return n, s3errs.ErrEntityTooSmall
	}
	return n, err
}

// postObject handles POST Object requests, the browser form upload. It
// authenticates itself rather than taking a key from the router, because the
// credential, the policy and the signature over it are fields of the body
// rather than headers.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html
func (s *s3) postObject(w http.ResponseWriter, r *http.Request, bucket string) error {
	form, file, err := parsePostForm(r)
	if err != nil {
		return err
	}

	accessKeyID, err := auth.PostPolicyAuth{
		Algorithm:  form.get(postFieldAlgorithm),
		Credential: form.get(postFieldCredential),
		Date:       form.get(postFieldDate),
		Policy:     form.get(postFieldPolicy),
		Signature:  form.get(postFieldSignature),
	}.Verify(r.Context(), s.backend, s.region, time.Now())
	if err != nil {
		return err
	}

	policy, err := parsePostPolicy(form.get(postFieldPolicy))
	if err != nil {
		return err
	}

	// the bucket condition is checked against the bucket in the URL, since a
	// form is not required to carry a bucket field, and one that does has to
	// agree with the URL
	if submitted, ok := form.fields[postFieldBucket]; ok && submitted != bucket {
		return s3errs.ErrInvalidPolicyDocument
	}
	form.fields[postFieldBucket] = bucket
	if err := policy.check(form, time.Now()); err != nil {
		return err
	}

	// the conditions are checked against the key as it was submitted, so the
	// filename is only substituted once they hold
	object := strings.ReplaceAll(form.get(postFieldKey), postFilenameVariable, form.filename)
	if object == "" {
		return s3errs.ErrUserKeyMustBeSpecified
	} else if len(object) > KeySizeLimit {
		return s3errs.ErrKeyTooLongError
	}

	// the request body bounds the file from above, and a content-length-range
	// condition bounds it further, so the backend has a size to reserve against
	// its disk limit
	maxSize := r.ContentLength
	if maxSize < 0 || (policy.maxSize >= 0 && policy.maxSize < maxSize) {
		maxSize = policy.maxSize
	}
	if maxSize < 0 {
		return s3errs.ErrMissingContentLength
	}

	meta, err := form.metadata()
	if err != nil {
		return err
	}

	// the redirect is resolved before the upload, so nothing that can fail is
	// left to run after the object has been stored
	var redirect *url.URL
	if target := form.redirect(); target != "" {
		// S3 ignores a redirect it cannot use rather than failing the upload
		redirect, _ = url.Parse(target)
	}

	log := s.logger.With(zap.String("bucket", bucket), zap.String("object", object))
	log.Debug("post object")

	res, err := s.backend.PutObject(r.Context(), accessKeyID, bucket, object, &postBody{
		reader: file,
		min:    policy.minSize,
	}, PutObjectOptions{
		ContentLength:    -1,
		MaxContentLength: maxSize,
		Meta:             meta,
	})
	if err != nil {
		return err
	}

	s.setLifecycleExpirationHeader(r.Context(), w, accessKeyID, bucket, object, time.Now())
	if res.VersionID != "" {
		w.Header().Set("x-amz-version-id", res.VersionID)
	}
	etag := FormatETag(res.ContentMD5[:], 0)
	w.Header().Set("ETag", etag)

	if redirect != nil {
		query := redirect.Query()
		query.Set("bucket", bucket)
		query.Set("key", object)
		query.Set("etag", etag)
		redirect.RawQuery = query.Encode()
		w.Header().Set("Location", redirect.String())
		w.WriteHeader(http.StatusSeeOther)
		return nil
	}

	switch form.get(postFieldStatus) {
	case strconv.Itoa(http.StatusOK):
		w.WriteHeader(http.StatusOK)
	case strconv.Itoa(http.StatusCreated):
		return writeXMLResponse(w, http.StatusCreated, PostObjectResponse{
			Location: s.objectLocation(r, bucket, object),
			Bucket:   bucket,
			Key:      object,
			ETag:     etag,
		})
	default:
		w.WriteHeader(http.StatusNoContent)
	}
	return nil
}
