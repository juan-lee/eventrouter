package containerregistry

// Copyright (c) Microsoft and contributors.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Code generated by Microsoft (R) AutoRest Code Generator.
// Changes may cause incorrect behavior and will be lost if the code is regenerated.

import (
	"encoding/json"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/date"
	"github.com/Azure/go-autorest/autorest/to"
	"net/http"
)

// Registry an object that represents a container registry.
type Registry struct {
	autorest.Response `json:"-"`
	// RegistryProperties - The properties of the container registry.
	*RegistryProperties `json:"properties,omitempty"`
	// ID - The resource ID.
	ID *string `json:"id,omitempty"`
	// Name - The name of the resource.
	Name *string `json:"name,omitempty"`
	// Type - The type of the resource.
	Type *string `json:"type,omitempty"`
	// Location - The location of the resource. This cannot be changed after the resource is created.
	Location *string `json:"location,omitempty"`
	// Tags - The tags of the resource.
	Tags map[string]*string `json:"tags"`
}

// MarshalJSON is the custom marshaler for Registry.
func (r Registry) MarshalJSON() ([]byte, error) {
	objectMap := make(map[string]interface{})
	if r.RegistryProperties != nil {
		objectMap["properties"] = r.RegistryProperties
	}
	if r.ID != nil {
		objectMap["id"] = r.ID
	}
	if r.Name != nil {
		objectMap["name"] = r.Name
	}
	if r.Type != nil {
		objectMap["type"] = r.Type
	}
	if r.Location != nil {
		objectMap["location"] = r.Location
	}
	if r.Tags != nil {
		objectMap["tags"] = r.Tags
	}
	return json.Marshal(objectMap)
}

// UnmarshalJSON is the custom unmarshaler for Registry struct.
func (r *Registry) UnmarshalJSON(body []byte) error {
	var m map[string]*json.RawMessage
	err := json.Unmarshal(body, &m)
	if err != nil {
		return err
	}
	for k, v := range m {
		switch k {
		case "properties":
			if v != nil {
				var registryProperties RegistryProperties
				err = json.Unmarshal(*v, &registryProperties)
				if err != nil {
					return err
				}
				r.RegistryProperties = &registryProperties
			}
		case "id":
			if v != nil {
				var ID string
				err = json.Unmarshal(*v, &ID)
				if err != nil {
					return err
				}
				r.ID = &ID
			}
		case "name":
			if v != nil {
				var name string
				err = json.Unmarshal(*v, &name)
				if err != nil {
					return err
				}
				r.Name = &name
			}
		case "type":
			if v != nil {
				var typeVar string
				err = json.Unmarshal(*v, &typeVar)
				if err != nil {
					return err
				}
				r.Type = &typeVar
			}
		case "location":
			if v != nil {
				var location string
				err = json.Unmarshal(*v, &location)
				if err != nil {
					return err
				}
				r.Location = &location
			}
		case "tags":
			if v != nil {
				var tags map[string]*string
				err = json.Unmarshal(*v, &tags)
				if err != nil {
					return err
				}
				r.Tags = tags
			}
		}
	}

	return nil
}

// RegistryCredentials the result of a request to get the administrator login credentials for a container registry.
type RegistryCredentials struct {
	autorest.Response `json:"-"`
	// Username - The administrator username.
	Username *string `json:"username,omitempty"`
	// Password - The administrator password.
	Password *string `json:"password,omitempty"`
}

// RegistryListResult the result of a request to list container registries.
type RegistryListResult struct {
	autorest.Response `json:"-"`
	// Value - The list of container registries. Since this list may be incomplete, the nextLink field should be used to request the next list of container registries.
	Value *[]Registry `json:"value,omitempty"`
	// NextLink - The URI that can be used to request the next list of container registries.
	NextLink *string `json:"nextLink,omitempty"`
}

// RegistryListResultIterator provides access to a complete listing of Registry values.
type RegistryListResultIterator struct {
	i    int
	page RegistryListResultPage
}

// Next advances to the next value.  If there was an error making
// the request the iterator does not advance and the error is returned.
func (iter *RegistryListResultIterator) Next() error {
	iter.i++
	if iter.i < len(iter.page.Values()) {
		return nil
	}
	err := iter.page.Next()
	if err != nil {
		iter.i--
		return err
	}
	iter.i = 0
	return nil
}

// NotDone returns true if the enumeration should be started or is not yet complete.
func (iter RegistryListResultIterator) NotDone() bool {
	return iter.page.NotDone() && iter.i < len(iter.page.Values())
}

// Response returns the raw server response from the last page request.
func (iter RegistryListResultIterator) Response() RegistryListResult {
	return iter.page.Response()
}

// Value returns the current value or a zero-initialized value if the
// iterator has advanced beyond the end of the collection.
func (iter RegistryListResultIterator) Value() Registry {
	if !iter.page.NotDone() {
		return Registry{}
	}
	return iter.page.Values()[iter.i]
}

// IsEmpty returns true if the ListResult contains no values.
func (rlr RegistryListResult) IsEmpty() bool {
	return rlr.Value == nil || len(*rlr.Value) == 0
}

// registryListResultPreparer prepares a request to retrieve the next set of results.
// It returns nil if no more results exist.
func (rlr RegistryListResult) registryListResultPreparer() (*http.Request, error) {
	if rlr.NextLink == nil || len(to.String(rlr.NextLink)) < 1 {
		return nil, nil
	}
	return autorest.Prepare(&http.Request{},
		autorest.AsJSON(),
		autorest.AsGet(),
		autorest.WithBaseURL(to.String(rlr.NextLink)))
}

// RegistryListResultPage contains a page of Registry values.
type RegistryListResultPage struct {
	fn  func(RegistryListResult) (RegistryListResult, error)
	rlr RegistryListResult
}

// Next advances to the next page of values.  If there was an error making
// the request the page does not advance and the error is returned.
func (page *RegistryListResultPage) Next() error {
	next, err := page.fn(page.rlr)
	if err != nil {
		return err
	}
	page.rlr = next
	return nil
}

// NotDone returns true if the page enumeration should be started or is not yet complete.
func (page RegistryListResultPage) NotDone() bool {
	return !page.rlr.IsEmpty()
}

// Response returns the raw server response from the last page request.
func (page RegistryListResultPage) Response() RegistryListResult {
	return page.rlr
}

// Values returns the slice of values for the current page or nil if there are no values.
func (page RegistryListResultPage) Values() []Registry {
	if page.rlr.IsEmpty() {
		return nil
	}
	return *page.rlr.Value
}

// RegistryNameCheckRequest a request to check whether the container registry name is available.
type RegistryNameCheckRequest struct {
	// Name - The name of the container registry.
	Name *string `json:"name,omitempty"`
	// Type - The resource type of the container registry. This field must be set to "Microsoft.ContainerRegistry/registries".
	Type *string `json:"type,omitempty"`
}

// RegistryNameStatus the result of a request to check the availability of a container registry name.
type RegistryNameStatus struct {
	autorest.Response `json:"-"`
	// NameAvailable - The value that indicates whether the name is available.
	NameAvailable *bool `json:"nameAvailable,omitempty"`
	// Reason - If any, the reason that the name is not available.
	Reason *string `json:"reason,omitempty"`
	// Message - If any, the error message that provides more detail for the reason that the name is not available.
	Message *string `json:"message,omitempty"`
}

// RegistryProperties the properties of a container registry.
type RegistryProperties struct {
	// LoginServer - The URL that can be used to log into the container registry.
	LoginServer *string `json:"loginServer,omitempty"`
	// CreationDate - The creation date of the container registry in ISO8601 format.
	CreationDate *date.Time `json:"creationDate,omitempty"`
	// AdminUserEnabled - The value that indicates whether the admin user is enabled. This value is false by default.
	AdminUserEnabled *bool `json:"adminUserEnabled,omitempty"`
	// StorageAccount - The properties of the storage account for the container registry. If specified, the storage account must be in the same physical location as the container registry.
	StorageAccount *StorageAccountProperties `json:"storageAccount,omitempty"`
}

// RegistryPropertiesUpdateParameters the parameters for updating the properties of a container registry.
type RegistryPropertiesUpdateParameters struct {
	// AdminUserEnabled - The value that indicates whether the admin user is enabled. This value is false by default.
	AdminUserEnabled *bool `json:"adminUserEnabled,omitempty"`
	// StorageAccount - The properties of a storage account for the container registry. If specified, the storage account must be in the same physical location as the container registry.
	StorageAccount *StorageAccountProperties `json:"storageAccount,omitempty"`
}

// RegistryUpdateParameters the parameters for updating a container registry.
type RegistryUpdateParameters struct {
	// Tags - The resource tags for the container registry.
	Tags map[string]*string `json:"tags"`
	// RegistryPropertiesUpdateParameters - The properties that the container registry will be updated with.
	*RegistryPropertiesUpdateParameters `json:"properties,omitempty"`
}

// MarshalJSON is the custom marshaler for RegistryUpdateParameters.
func (rup RegistryUpdateParameters) MarshalJSON() ([]byte, error) {
	objectMap := make(map[string]interface{})
	if rup.Tags != nil {
		objectMap["tags"] = rup.Tags
	}
	if rup.RegistryPropertiesUpdateParameters != nil {
		objectMap["properties"] = rup.RegistryPropertiesUpdateParameters
	}
	return json.Marshal(objectMap)
}

// UnmarshalJSON is the custom unmarshaler for RegistryUpdateParameters struct.
func (rup *RegistryUpdateParameters) UnmarshalJSON(body []byte) error {
	var m map[string]*json.RawMessage
	err := json.Unmarshal(body, &m)
	if err != nil {
		return err
	}
	for k, v := range m {
		switch k {
		case "tags":
			if v != nil {
				var tags map[string]*string
				err = json.Unmarshal(*v, &tags)
				if err != nil {
					return err
				}
				rup.Tags = tags
			}
		case "properties":
			if v != nil {
				var registryPropertiesUpdateParameters RegistryPropertiesUpdateParameters
				err = json.Unmarshal(*v, &registryPropertiesUpdateParameters)
				if err != nil {
					return err
				}
				rup.RegistryPropertiesUpdateParameters = &registryPropertiesUpdateParameters
			}
		}
	}

	return nil
}

// Resource an Azure resource.
type Resource struct {
	// ID - The resource ID.
	ID *string `json:"id,omitempty"`
	// Name - The name of the resource.
	Name *string `json:"name,omitempty"`
	// Type - The type of the resource.
	Type *string `json:"type,omitempty"`
	// Location - The location of the resource. This cannot be changed after the resource is created.
	Location *string `json:"location,omitempty"`
	// Tags - The tags of the resource.
	Tags map[string]*string `json:"tags"`
}

// MarshalJSON is the custom marshaler for Resource.
func (r Resource) MarshalJSON() ([]byte, error) {
	objectMap := make(map[string]interface{})
	if r.ID != nil {
		objectMap["id"] = r.ID
	}
	if r.Name != nil {
		objectMap["name"] = r.Name
	}
	if r.Type != nil {
		objectMap["type"] = r.Type
	}
	if r.Location != nil {
		objectMap["location"] = r.Location
	}
	if r.Tags != nil {
		objectMap["tags"] = r.Tags
	}
	return json.Marshal(objectMap)
}

// StorageAccountProperties the properties of a storage account for a container registry.
type StorageAccountProperties struct {
	// Name - The name of the storage account.
	Name *string `json:"name,omitempty"`
	// AccessKey - The access key to the storage account.
	AccessKey *string `json:"accessKey,omitempty"`
}
