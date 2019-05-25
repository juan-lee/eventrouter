// +build go1.9

// Copyright 2019 Microsoft Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This code was auto-generated by:
// github.com/Azure/azure-sdk-for-go/tools/profileBuilder

package operationalinsights

import original "github.com/Azure/azure-sdk-for-go/services/operationalinsights/mgmt/2015-03-20/operationalinsights"

const (
	DefaultBaseURI = original.DefaultBaseURI
)

type BaseClient = original.BaseClient
type PurgeState = original.PurgeState

const (
	Completed PurgeState = original.Completed
	Pending   PurgeState = original.Pending
)

type SearchSortEnum = original.SearchSortEnum

const (
	Asc  SearchSortEnum = original.Asc
	Desc SearchSortEnum = original.Desc
)

type StorageInsightState = original.StorageInsightState

const (
	ERROR StorageInsightState = original.ERROR
	OK    StorageInsightState = original.OK
)

type CoreSummary = original.CoreSummary
type LinkTarget = original.LinkTarget
type ListLinkTarget = original.ListLinkTarget
type Operation = original.Operation
type OperationDisplay = original.OperationDisplay
type OperationListResult = original.OperationListResult
type ProxyResource = original.ProxyResource
type Resource = original.Resource
type SavedSearch = original.SavedSearch
type SavedSearchesListResult = original.SavedSearchesListResult
type SavedSearchProperties = original.SavedSearchProperties
type SearchError = original.SearchError
type SearchGetSchemaResponse = original.SearchGetSchemaResponse
type SearchHighlight = original.SearchHighlight
type SearchMetadata = original.SearchMetadata
type SearchMetadataSchema = original.SearchMetadataSchema
type SearchParameters = original.SearchParameters
type SearchResultsResponse = original.SearchResultsResponse
type SearchSchemaValue = original.SearchSchemaValue
type SearchSort = original.SearchSort
type StorageAccount = original.StorageAccount
type StorageInsight = original.StorageInsight
type StorageInsightListResult = original.StorageInsightListResult
type StorageInsightListResultIterator = original.StorageInsightListResultIterator
type StorageInsightListResultPage = original.StorageInsightListResultPage
type StorageInsightProperties = original.StorageInsightProperties
type StorageInsightStatus = original.StorageInsightStatus
type Tag = original.Tag
type WorkspacePurgeBody = original.WorkspacePurgeBody
type WorkspacePurgeBodyFilters = original.WorkspacePurgeBodyFilters
type WorkspacePurgeResponse = original.WorkspacePurgeResponse
type WorkspacePurgeStatusResponse = original.WorkspacePurgeStatusResponse
type WorkspacesGetSearchResultsFuture = original.WorkspacesGetSearchResultsFuture
type OperationsClient = original.OperationsClient
type SavedSearchesClient = original.SavedSearchesClient
type StorageInsightsClient = original.StorageInsightsClient
type WorkspacesClient = original.WorkspacesClient

func New(subscriptionID string, purgeID string) BaseClient {
	return original.New(subscriptionID, purgeID)
}
func NewWithBaseURI(baseURI string, subscriptionID string, purgeID string) BaseClient {
	return original.NewWithBaseURI(baseURI, subscriptionID, purgeID)
}
func PossiblePurgeStateValues() []PurgeState {
	return original.PossiblePurgeStateValues()
}
func PossibleSearchSortEnumValues() []SearchSortEnum {
	return original.PossibleSearchSortEnumValues()
}
func PossibleStorageInsightStateValues() []StorageInsightState {
	return original.PossibleStorageInsightStateValues()
}
func NewOperationsClient(subscriptionID string, purgeID string) OperationsClient {
	return original.NewOperationsClient(subscriptionID, purgeID)
}
func NewOperationsClientWithBaseURI(baseURI string, subscriptionID string, purgeID string) OperationsClient {
	return original.NewOperationsClientWithBaseURI(baseURI, subscriptionID, purgeID)
}
func NewSavedSearchesClient(subscriptionID string, purgeID string) SavedSearchesClient {
	return original.NewSavedSearchesClient(subscriptionID, purgeID)
}
func NewSavedSearchesClientWithBaseURI(baseURI string, subscriptionID string, purgeID string) SavedSearchesClient {
	return original.NewSavedSearchesClientWithBaseURI(baseURI, subscriptionID, purgeID)
}
func NewStorageInsightsClient(subscriptionID string, purgeID string) StorageInsightsClient {
	return original.NewStorageInsightsClient(subscriptionID, purgeID)
}
func NewStorageInsightsClientWithBaseURI(baseURI string, subscriptionID string, purgeID string) StorageInsightsClient {
	return original.NewStorageInsightsClientWithBaseURI(baseURI, subscriptionID, purgeID)
}
func UserAgent() string {
	return original.UserAgent() + " profiles/latest"
}
func Version() string {
	return original.Version()
}
func NewWorkspacesClient(subscriptionID string, purgeID string) WorkspacesClient {
	return original.NewWorkspacesClient(subscriptionID, purgeID)
}
func NewWorkspacesClientWithBaseURI(baseURI string, subscriptionID string, purgeID string) WorkspacesClient {
	return original.NewWorkspacesClientWithBaseURI(baseURI, subscriptionID, purgeID)
}
