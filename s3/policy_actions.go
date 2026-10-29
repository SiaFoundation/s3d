package s3

import "strings"

// The resource types an action applies to in a bucket policy.
const (
	// resourceTypeNone marks actions only a "*" resource covers
	resourceTypeNone   = ""
	resourceTypeBucket = "bucket"
	resourceTypeObject = "object"
)

// policyActionResources maps every S3 action, lowercase, to the resource type
// it applies to. It includes actions s3d has no API for, so AWS policies that
// name them still load.
//
// https://servicereference.us-east-1.amazonaws.com/v1/s3/s3.json
var policyActionResources = map[string]string{
	"s3:abortmultipartupload":                             resourceTypeObject,
	"s3:allowvendedlogdeliveryforresource":                resourceTypeBucket,
	"s3:associateaccessgrantsidentitycenter":              resourceTypeNone,
	"s3:bypassgovernanceretention":                        resourceTypeObject,
	"s3:createaccessgrant":                                resourceTypeNone,
	"s3:createaccessgrantsinstance":                       resourceTypeNone,
	"s3:createaccessgrantslocation":                       resourceTypeNone,
	"s3:createaccesspoint":                                resourceTypeNone,
	"s3:createaccesspointforobjectlambda":                 resourceTypeNone,
	"s3:createbucket":                                     resourceTypeBucket,
	"s3:createbucketmetadatatableconfiguration":           resourceTypeBucket,
	"s3:createjob":                                        resourceTypeNone,
	"s3:createmultiregionaccesspoint":                     resourceTypeNone,
	"s3:createstoragelensgroup":                           resourceTypeNone,
	"s3:deleteaccessgrant":                                resourceTypeNone,
	"s3:deleteaccessgrantsinstance":                       resourceTypeNone,
	"s3:deleteaccessgrantsinstanceresourcepolicy":         resourceTypeNone,
	"s3:deleteaccessgrantslocation":                       resourceTypeNone,
	"s3:deleteaccesspoint":                                resourceTypeNone,
	"s3:deleteaccesspointforobjectlambda":                 resourceTypeNone,
	"s3:deleteaccesspointpolicy":                          resourceTypeNone,
	"s3:deleteaccesspointpolicyforobjectlambda":           resourceTypeNone,
	"s3:deletebucket":                                     resourceTypeBucket,
	"s3:deletebucketmetadatatableconfiguration":           resourceTypeBucket,
	"s3:deletebucketpolicy":                               resourceTypeBucket,
	"s3:deletebucketwebsite":                              resourceTypeBucket,
	"s3:deletejobtagging":                                 resourceTypeNone,
	"s3:deletemultiregionaccesspoint":                     resourceTypeNone,
	"s3:deleteobject":                                     resourceTypeObject,
	"s3:deleteobjectannotation":                           resourceTypeObject,
	"s3:deleteobjecttagging":                              resourceTypeObject,
	"s3:deleteobjectversion":                              resourceTypeObject,
	"s3:deleteobjectversionannotation":                    resourceTypeObject,
	"s3:deleteobjectversiontagging":                       resourceTypeObject,
	"s3:deletestoragelensconfiguration":                   resourceTypeNone,
	"s3:deletestoragelensconfigurationtagging":            resourceTypeNone,
	"s3:deletestoragelensgroup":                           resourceTypeNone,
	"s3:describejob":                                      resourceTypeNone,
	"s3:describemultiregionaccesspointoperation":          resourceTypeNone,
	"s3:dissociateaccessgrantsidentitycenter":             resourceTypeNone,
	"s3:getaccelerateconfiguration":                       resourceTypeBucket,
	"s3:getaccessgrant":                                   resourceTypeNone,
	"s3:getaccessgrantsinstance":                          resourceTypeNone,
	"s3:getaccessgrantsinstanceforprefix":                 resourceTypeNone,
	"s3:getaccessgrantsinstanceresourcepolicy":            resourceTypeNone,
	"s3:getaccessgrantslocation":                          resourceTypeNone,
	"s3:getaccesspoint":                                   resourceTypeNone,
	"s3:getaccesspointconfigurationforobjectlambda":       resourceTypeNone,
	"s3:getaccesspointforobjectlambda":                    resourceTypeNone,
	"s3:getaccesspointpolicy":                             resourceTypeNone,
	"s3:getaccesspointpolicyforobjectlambda":              resourceTypeNone,
	"s3:getaccesspointpolicystatus":                       resourceTypeNone,
	"s3:getaccesspointpolicystatusforobjectlambda":        resourceTypeNone,
	"s3:getaccountpublicaccessblock":                      resourceTypeNone,
	"s3:getanalyticsconfiguration":                        resourceTypeBucket,
	"s3:getbucketabac":                                    resourceTypeBucket,
	"s3:getbucketacl":                                     resourceTypeBucket,
	"s3:getbucketcors":                                    resourceTypeBucket,
	"s3:getbucketlocation":                                resourceTypeBucket,
	"s3:getbucketlogging":                                 resourceTypeBucket,
	"s3:getbucketmetadatatableconfiguration":              resourceTypeBucket,
	"s3:getbucketnotification":                            resourceTypeBucket,
	"s3:getbucketobjectlockconfiguration":                 resourceTypeBucket,
	"s3:getbucketownershipcontrols":                       resourceTypeBucket,
	"s3:getbucketpolicy":                                  resourceTypeBucket,
	"s3:getbucketpolicystatus":                            resourceTypeBucket,
	"s3:getbucketpublicaccessblock":                       resourceTypeBucket,
	"s3:getbucketrequestpayment":                          resourceTypeBucket,
	"s3:getbuckettagging":                                 resourceTypeBucket,
	"s3:getbucketversioning":                              resourceTypeBucket,
	"s3:getbucketwebsite":                                 resourceTypeBucket,
	"s3:getdataaccess":                                    resourceTypeNone,
	"s3:getencryptionconfiguration":                       resourceTypeBucket,
	"s3:getintelligenttieringconfiguration":               resourceTypeBucket,
	"s3:getinventoryconfiguration":                        resourceTypeBucket,
	"s3:getjobtagging":                                    resourceTypeNone,
	"s3:getlifecycleconfiguration":                        resourceTypeBucket,
	"s3:getmetricsconfiguration":                          resourceTypeBucket,
	"s3:getmultiregionaccesspoint":                        resourceTypeNone,
	"s3:getmultiregionaccesspointpolicy":                  resourceTypeNone,
	"s3:getmultiregionaccesspointpolicystatus":            resourceTypeNone,
	"s3:getmultiregionaccesspointroutes":                  resourceTypeNone,
	"s3:getobject":                                        resourceTypeObject,
	"s3:getobjectacl":                                     resourceTypeObject,
	"s3:getobjectannotation":                              resourceTypeObject,
	"s3:getobjectattributes":                              resourceTypeObject,
	"s3:getobjectlegalhold":                               resourceTypeObject,
	"s3:getobjectretention":                               resourceTypeObject,
	"s3:getobjecttagging":                                 resourceTypeObject,
	"s3:getobjecttorrent":                                 resourceTypeObject,
	"s3:getobjectversion":                                 resourceTypeObject,
	"s3:getobjectversionacl":                              resourceTypeObject,
	"s3:getobjectversionannotation":                       resourceTypeObject,
	"s3:getobjectversionannotationforreplication":         resourceTypeObject,
	"s3:getobjectversionattributes":                       resourceTypeObject,
	"s3:getobjectversionforreplication":                   resourceTypeObject,
	"s3:getobjectversiontagging":                          resourceTypeObject,
	"s3:getobjectversiontorrent":                          resourceTypeObject,
	"s3:getreplicationconfiguration":                      resourceTypeBucket,
	"s3:getstoragelensconfiguration":                      resourceTypeNone,
	"s3:getstoragelensconfigurationtagging":               resourceTypeNone,
	"s3:getstoragelensdashboard":                          resourceTypeNone,
	"s3:getstoragelensgroup":                              resourceTypeNone,
	"s3:initiatereplication":                              resourceTypeObject,
	"s3:listaccessgrants":                                 resourceTypeNone,
	"s3:listaccessgrantsinstances":                        resourceTypeNone,
	"s3:listaccessgrantslocations":                        resourceTypeNone,
	"s3:listaccesspoints":                                 resourceTypeNone,
	"s3:listaccesspointsforobjectlambda":                  resourceTypeNone,
	"s3:listallmybuckets":                                 resourceTypeNone,
	"s3:listbucket":                                       resourceTypeBucket,
	"s3:listbucketmultipartuploads":                       resourceTypeBucket,
	"s3:listbucketversions":                               resourceTypeBucket,
	"s3:listcalleraccessgrants":                           resourceTypeNone,
	"s3:listjobs":                                         resourceTypeNone,
	"s3:listmultipartuploadparts":                         resourceTypeObject,
	"s3:listmultiregionaccesspoints":                      resourceTypeNone,
	"s3:listobjectannotations":                            resourceTypeObject,
	"s3:listobjectversionannotations":                     resourceTypeObject,
	"s3:liststoragelensconfigurations":                    resourceTypeNone,
	"s3:liststoragelensgroups":                            resourceTypeNone,
	"s3:listtagsforresource":                              resourceTypeBucket,
	"s3:objectowneroverridetobucketowner":                 resourceTypeObject,
	"s3:pausereplication":                                 resourceTypeBucket,
	"s3:putaccelerateconfiguration":                       resourceTypeBucket,
	"s3:putaccessgrantsinstanceresourcepolicy":            resourceTypeNone,
	"s3:putaccesspointconfigurationforobjectlambda":       resourceTypeNone,
	"s3:putaccesspointpolicy":                             resourceTypeNone,
	"s3:putaccesspointpolicyforobjectlambda":              resourceTypeNone,
	"s3:putaccesspointpublicaccessblock":                  resourceTypeNone,
	"s3:putaccountpublicaccessblock":                      resourceTypeNone,
	"s3:putanalyticsconfiguration":                        resourceTypeBucket,
	"s3:putbucketabac":                                    resourceTypeBucket,
	"s3:putbucketacl":                                     resourceTypeBucket,
	"s3:putbucketcors":                                    resourceTypeBucket,
	"s3:putbucketlogging":                                 resourceTypeBucket,
	"s3:putbucketnotification":                            resourceTypeBucket,
	"s3:putbucketobjectlockconfiguration":                 resourceTypeBucket,
	"s3:putbucketownershipcontrols":                       resourceTypeBucket,
	"s3:putbucketpolicy":                                  resourceTypeBucket,
	"s3:putbucketpublicaccessblock":                       resourceTypeBucket,
	"s3:putbucketrequestpayment":                          resourceTypeBucket,
	"s3:putbuckettagging":                                 resourceTypeBucket,
	"s3:putbucketversioning":                              resourceTypeBucket,
	"s3:putbucketwebsite":                                 resourceTypeBucket,
	"s3:putencryptionconfiguration":                       resourceTypeBucket,
	"s3:putintelligenttieringconfiguration":               resourceTypeBucket,
	"s3:putinventoryconfiguration":                        resourceTypeBucket,
	"s3:putjobtagging":                                    resourceTypeNone,
	"s3:putlifecycleconfiguration":                        resourceTypeBucket,
	"s3:putmetricsconfiguration":                          resourceTypeBucket,
	"s3:putmultiregionaccesspointpolicy":                  resourceTypeNone,
	"s3:putobject":                                        resourceTypeObject,
	"s3:putobjectacl":                                     resourceTypeObject,
	"s3:putobjectannotation":                              resourceTypeObject,
	"s3:putobjectlegalhold":                               resourceTypeObject,
	"s3:putobjectretention":                               resourceTypeObject,
	"s3:putobjecttagging":                                 resourceTypeObject,
	"s3:putobjectversionacl":                              resourceTypeObject,
	"s3:putobjectversionannotation":                       resourceTypeObject,
	"s3:putobjectversiontagging":                          resourceTypeObject,
	"s3:putreplicationconfiguration":                      resourceTypeBucket,
	"s3:putstoragelensconfiguration":                      resourceTypeNone,
	"s3:putstoragelensconfigurationtagging":               resourceTypeNone,
	"s3:replicatedelete":                                  resourceTypeObject,
	"s3:replicateobject":                                  resourceTypeObject,
	"s3:replicateobjectannotation":                        resourceTypeObject,
	"s3:replicatetags":                                    resourceTypeObject,
	"s3:restoreobject":                                    resourceTypeObject,
	"s3:submitmultiregionaccesspointroutes":               resourceTypeNone,
	"s3:tagresource":                                      resourceTypeBucket,
	"s3:untagresource":                                    resourceTypeBucket,
	"s3:updateaccessgrantslocation":                       resourceTypeNone,
	"s3:updatebucketmetadataannotationtableconfiguration": resourceTypeBucket,
	"s3:updatebucketmetadatainventorytableconfiguration":  resourceTypeBucket,
	"s3:updatebucketmetadatajournaltableconfiguration":    resourceTypeBucket,
	"s3:updatejobpriority":                                resourceTypeNone,
	"s3:updatejobstatus":                                  resourceTypeNone,
	"s3:updateobjectencryption":                           resourceTypeObject,
	"s3:updatestoragelensgroup":                           resourceTypeNone,
}

// validPolicyAction reports whether pattern matches a known action.
func validPolicyAction(pattern string) bool {
	pattern = strings.ToLower(pattern)
	for action := range policyActionResources {
		if wildcardMatch(pattern, action) {
			return true
		}
	}
	return false
}

// actionsApply reports whether an action the patterns match applies to one of
// resources. S3 rejects a statement whose actions apply to none of them.
func actionsApply(patterns, resources policyStrings) bool {
	for _, pattern := range patterns {
		pattern = strings.ToLower(pattern)
		for action, kind := range policyActionResources {
			if !wildcardMatch(pattern, action) {
				continue
			}
			for _, resource := range resources {
				if resourceHasType(resource, kind) {
					return true
				}
			}
		}
	}
	return false
}

// resourceHasType reports whether resource can name a resource of kind. A
// bucket name cannot contain '/', so only a '/' or a wildcard reaches objects.
func resourceHasType(resource, kind string) bool {
	if resource == policyWildcard {
		return true
	}
	name := strings.TrimPrefix(resource, policyARNPrefix)
	switch kind {
	case resourceTypeBucket:
		return !strings.Contains(name, "/")
	case resourceTypeObject:
		return strings.ContainsAny(name, "/*")
	}
	return false
}
