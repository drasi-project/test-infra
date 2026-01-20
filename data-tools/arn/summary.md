# Azure Resource Notifications (ARN) and Azure Resource Graph (ARG) Architecture Summary

## Overview

The GroupsRP service processes Azure Resource Notifications (ARN) events and uses Azure Resource Graph (ARG) for validation and cleanup operations. The system manages relationships between Azure resources, particularly for Management Groups and Service Groups functionality.

**Key Finding**: The system is **fully compliant with ARN Schema V3** specification, supporting both inline resource notifications and blob URI notifications with comprehensive field mapping.

## ARN (Azure Resource Notification) Data Flow

### Data Source
- **Source**: Azure Event Grid
- **Entry Point**: `NotificationWriterService/Controllers/ArnEventsController.cs`
- **Authentication**: Service-to-Service (S2S) with Azure Event Grid webhook role
- **Endpoints**:
  - `/providers/Microsoft.Management/arnEvents`
  - `/providers/Microsoft.Management/events`
  - `/arn/arnEvents`
  - `/arn/events`

### Supported ARN Event Types

#### 1. Tracked Resource Notifications (from ARM)
- **What**: Direct Azure resources (Management Groups, Service Groups)
- **Source**: Azure Resource Manager (ARM)
- **Event Types**:
  - Resource Write Events
  - Resource Delete Events
- **Processing Method**: `ProcessInlineTrackedResourceNotification()`

#### 2. Proxy Resource Notifications (Relationships)
- **What**: Relationship resources (Service Group Members, Dependencies)
- **Source**: ARM or relationship providers
- **Event Types**:
  - Relationship Write/Snapshot Events
  - Relationship Delete Events
- **Processing Method**: `ProcessProxyResourceNotification()`

#### 3. Blob Notifications
- **What**: Large-scale or batch notifications
- **Source**: Azure Storage
- **Processing Method**: `ProcessBlobNotification()`

### ARN Processing Pipeline

```
Event Grid → ArnEventsController → Event Classification → Data Extraction → Validation (ARG) → Storage (Cosmos DB)
```

1. **Event Reception**: Events received via HTTP POST from Event Grid
2. **Event Parsing**: `EventGridEvent.ParseMany()` parses incoming events
3. **Type Detection**: System determines event category using helper methods
4. **Data Extraction**: Resource IDs, tenant info, and metadata extracted
5. **Channel Processing**: Events queued in appropriate processing channels
6. **Storage**: Valid data stored in Cosmos DB after processing

### Processing Channels

#### Tracked Resources
- **Write Channel**: `TrackedResourceWriteChannel`
- **Delete Channel**: `TrackedResourceDeleteChannel`
- **Processors**: 
  - `TrackedResourceWriter` → Cosmos DB
  - `TrackedResourceRemover` → Cosmos DB

#### Relationship Resources
- **Write Channel**: `RelationshipResourceWriteChannel`
- **Delete Channel**: `RelationshipResourceDeleteChannel`
- **Processors**:
  - `RelationshipWriteProcessor` → Cosmos DB
  - `RelationshipDeleteProcessor` → Cosmos DB

## ARN Schema V3 Compliance

### Data Model Implementation

The system implements comprehensive ARN Schema V3 compliance through several model classes:

#### Main Event Data (`ArnEventData.cs`)
```csharp
public class ArnEventData
{
    [JsonProperty("homeTenantId")]
    public string? HomeTenantId { get; set; }

    [JsonProperty("resourceHomeTenantId")]
    public string? ResourceHomeTenantId { get; set; }

    [JsonProperty("resourcesContainer")]
    public string? ResourcesContainer { get; set; }

    [JsonProperty("resourceLocation")]
    public string? ResourceLocation { get; set; }

    [JsonProperty("publisherInfo")]
    public string? PublisherInfo { get; set; }

    [JsonProperty("apiVersion")]
    public string? ApiVersion { get; set; }

    [JsonProperty("resources")]
    public ArnResourcesData[]? Resources { get; set; }

    [JsonProperty("resourcesBlobInfo")]
    public ArnResourcesBlobInfo? ResourcesBlobInfo { get; set; }

    [JsonProperty("additionalBatchProperties")]
    public NotificationsBatchProperties? AdditionalBatchProperties { get; set; }

    // Helper method to determine event type
    public bool IsInlineResourceNotification() => Resources?.Length > 0;
}
```

#### Resource Data (`ArnResourcesData.cs`)
```csharp
public class ArnResourcesData
{
    [JsonProperty("resourceId", Required = Required.Always)]
    public string ResourceId { get; set; } = string.Empty;

    [JsonProperty("correlationId", Required = Required.Always)]
    public string CorrelationId { get; set; } = string.Empty;

    [JsonProperty("resourceEventTime")]
    public DateTime? ResourceEventTime { get; set; }

    [JsonProperty("homeTenantId")]
    public string? HomeTenantId { get; set; }

    [JsonProperty("armResource")]
    public JObject? ArmResource { get; set; }

    [JsonProperty("resourceSystemProperties")]
    public ArnResourceSystemProperties? ResourceSystemProperties { get; set; }
}
```

### Dual Event Format Support
- **Inline Resources**: Events with `resources` array for immediate processing
- **Blob URI**: Events with `resourcesBlobInfo` for large batches stored in blob storage

## ARG (Azure Resource Graph) Role

### Purpose
ARG provides **validation and cleanup services** - it does NOT store data but validates data integrity.

### Key Functions

#### 1. Resource Existence Validation
- **Purpose**: Verify target resources exist before creating relationships
- **Query Type**: Source ID validation in tenant

#### 2. Cleanup Operations
- **Purpose**: Find orphaned Service Group Member relationships
- **Query**: Find relationships where source resources no longer exist

#### 3. Resource Count Validation
- **Purpose**: Validate subscription limits before resource creation
- **Query**: Count resources by type and subscription

### Key ARG Queries

```kusto
-- Service Group Member Cleanup
RelationshipResources
    | where type =~ "microsoft.relationships/servicegroupmember"
    | where tenantId =~ "{tenantId}"
    | extend sourceId = properties.SourceId
    | project sourceId, id

-- Resource Count Validation
{table}
    | where type =~ "{resourceType}"
    | summarize count() by type, subscriptionId
    | project count=count_, subscriptionId, type

-- Source ID Validation
Resources
    | where id in~ ({sourceIds})
    | where tenantId =~ "{tenantId}"
    | project id
```

## Data Storage Architecture

### Cosmos DB (Primary Storage)
- **Role**: Write-through cache and persistent storage
- **Content**: 
  - Resource entries (Management Groups, Service Groups metadata)
  - Relationship data (Service Group Member relationships, dependencies)
  - Processed notification state
- **Database**: `ResourceEntryDataProvider.DatabaseId`
- **Container**: `ResourceEntryDataProvider.ResourceContainerId`

### SQL Server (Secondary Storage)
- **Role**: Service Groups metadata and configuration
- **Use Case**: Traditional relational data requiring ACID transactions

## Background Processing

### Key Jobs
- **SingleTenantServiceGroupMemberCleanupJob**: Uses ARG to find and clean orphaned relationships
- **ServiceGroupMemberCRUDJob**: Handles CRUD operations for Service Group Members
- **SyncSubscriptionWithArmJob**: Synchronizes subscription data from ARM

### Processing Flow
1. **Background Service**: `NotificationProcessingBackgroundService` manages lifecycle
2. **Channel Readers**: Process queued events asynchronously
3. **Retry Logic**: Built-in retry for transient failures
4. **Error Handling**: Dead letter queuing for failed operations

## Key Architectural Patterns

### Data Flow Direction
- **ARN**: Event Grid → NotificationWriterService → Cosmos DB (Inbound)
- **ARG**: GroupsRP → Azure Resource Graph (Outbound queries)
- **Cosmos DB**: Local storage for processed data (Storage)

### Why This Architecture?
1. **Scalability**: Asynchronous processing handles high-volume notifications
2. **Reliability**: Channels provide buffering and retry capabilities
3. **Consistency**: ARG queries ensure relationships point to valid resources
4. **Multi-tenancy**: Supports resource management across Azure tenants
5. **Real-time Updates**: Immediate notification processing keeps data current

## Key Processing Functions by ARN Event Type

### 1. Event Reception & Classification (`ArnEventsController.cs`)
- `EventGridEvent.ParseMany()` - Parse incoming Event Grid events
- `eventGridEvent.GetData<ArnEventData>()` - Extract ARN data from event
- `ProcessInlineTrackedResourceNotification()` - Handle inline resource events
- `ProcessBlobNotification()` - Handle blob URI events  
- `ProcessProxyResourceNotification()` - Handle relationship events

### 2. Tracked Resource Processing (`TrackedResourceWriter.cs`)
- `ProcessItemAsync()` - Main processing entry point
- `AsyncRetry.ExecuteAsync()` - Retry wrapper for reliability
- `resourceEntryDataProvider.CreateOrUpdateResource()` - Write to Cosmos DB
- `resourceEntryDataProvider.DeleteResource()` - Remove from Cosmos DB

### 3. Relationship Processing (`RelationshipWriteProcessor.cs`)
- `HandleRelationshipWrite()` - Main relationship creation
- `resourceEntryDataProvider.CreateAffinitySlot()` - Create source/target slots
- `edgeDataProvider.CreateEdge()` - Create relationship edge
- `HandleRelationshipDelete()` - Remove relationships

### 4. ARG Validation (`ResourceGraphEngine.cs`)
- `QueryAsync()` - Execute Kusto queries against ARG
- `ValidateSourceResourceIds()` - Verify resources exist
- `GetAllServiceGroupMembersInTenant()` - Find orphaned relationships
- `GetSubscriptionResourceCount()` - Validate resource limits

### 5. Background Processing (`NotificationProcessingBackgroundService`)
- `StartAsync()` - Initialize processing channels
- `ExecuteAsync()` - Main processing loop
- `ProcessTrackedResourceWrites()` - Handle write channel
- `ProcessRelationshipWrites()` - Handle relationship channel

### 6. Event Classification Helpers (`ArnEventData.cs`)
- `IsInlineResourceNotification()` - Check for inline resources
- `IsBlobResourceNotification()` - Check for blob URI format
- `IsTrackedResourceEvent()` - Identify ARM resource events
- `IsRelationshipEvent()` - Identify relationship events

## Development Dependencies

### Cosmos DB Emulator
- **Required for**: Local development and testing
- **Reason**: NotificationWriterService needs document storage for relationship graphs
- **Configuration**: Special startup parameters for high throughput testing
- **Initialization**: `CosmosDbDevelopmentInitializer` sets up development database/containers

### Configuration Files
- **Location**: `GroupsWorker.Tests/app.config`, deployment configs
- **ARG Settings**: Base service URI, API version, timeouts, retry policies
- **Cosmos Settings**: Connection strings, partition strategies, TTL settings


Based on the GroupsRP implementation, your test data should follow this pattern:
```json
{
  "homeTenantId": "your-tenant-id",
  "resourceHomeTenantId": "resource-tenant-id", 
  "resourcesContainer": "Inline", // or "Blob"
  "resourceLocation": "global",
  "publisherInfo": "Microsoft.Management", 
  "apiVersion": "2021-04-01",
  "resources": [
    {
      "resourceId": "/subscriptions/{sub-id}/resourceGroups/{rg}/providers/Microsoft.Management/managementGroups/{mg-id}",
      "correlationId": "guid-here",
      "resourceEventTime": "2025-11-14T20:30:00Z",
      "homeTenantId": "tenant-id",
      "statusCode": "200",
      "armResource": {
        "id": "/subscriptions/{sub-id}/resourceGroups/{rg}/providers/Microsoft.Management/managementGroups/{mg-id}",
        "name": "my-management-group",
        "type": "Microsoft.Management/managementGroups",
        "properties": {
          "displayName": "My Management Group",
          "details": {
            "parent": {
              "id": "/providers/Microsoft.Management/managementGroups/parent-id"
            }
          }
        }
      },
      "resourceSystemProperties": {
        "createdBy": "user@domain.com",
        "createdTime": "2025-11-14T20:30:00Z",
        "changeAction": "Create"
      }
    }
  ]
}{
  "homeTenantId": "your-tenant-id",
  "resourceHomeTenantId": "resource-tenant-id", 
  "resourcesContainer": "Inline", // or "Blob"
  "resourceLocation": "global",
  "publisherInfo": "Microsoft.Management", 
  "apiVersion": "2021-04-01",
  "resources": [
    {
      "resourceId": "/subscriptions/{sub-id}/resourceGroups/{rg}/providers/Microsoft.Management/managementGroups/{mg-id}",
      "correlationId": "guid-here",
      "resourceEventTime": "2025-11-14T20:30:00Z",
      "homeTenantId": "tenant-id",
      "statusCode": "200",
      "armResource": {
        "id": "/subscriptions/{sub-id}/resourceGroups/{rg}/providers/Microsoft.Management/managementGroups/{mg-id}",
        "name": "my-management-group",
        "type": "Microsoft.Management/managementGroups",
        "properties": {
          "displayName": "My Management Group",
          "details": {
            "parent": {
              "id": "/providers/Microsoft.Management/managementGroups/parent-id"
            }
          }
        }
      },
      "resourceSystemProperties": {
        "createdBy": "user@domain.com",
        "createdTime": "2025-11-14T20:30:00Z",
        "changeAction": "Create"
      }
    }
  ]
}
```