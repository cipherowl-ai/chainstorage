# ChainStorage Component Diagram

## Overview

ChainStorage is a distributed blockchain data storage and processing system that continuously replicates blockchain data and serves it through APIs.

```mermaid
graph TB
    %% External Systems
    subgraph "External Systems"
        BC[Blockchain Nodes<br/>Bitcoin/Ethereum/Solana/etc]
        Client[External Clients<br/>SDK/REST/gRPC]
        Temporal[Temporal Server]
        AWS[AWS Services<br/>S3/DynamoDB/SQS]
        GCP[GCP Services<br/>GCS/Firestore]
    end

    %% Entry Points
    subgraph "Entry Points (cmd/)"
        API[API Server<br/>REST/gRPC]
        Server[Main Server<br/>Workflow Manager]
        Worker[Worker<br/>Activity Executor]
        Admin[Admin CLI]
        Cron[Cron Jobs]
    end

    %% Core Services
    subgraph "Core Services"
        subgraph "Storage Layer"
            BlobStorage[BlobStorage<br/>Raw Block Data]
            MetaStorage[MetaStorage<br/>Metadata & Indexes]
        end

        subgraph "Blockchain Layer"
            ClientFactory[Client Factory]
            subgraph "Multi-Endpoint Clients"
                Master[Master Client<br/>Sticky Sessions]
                Slave[Slave Client<br/>Load Balanced]
                Validator[Validator Client]
                Consensus[Consensus Client]
            end
            
            subgraph "Parsers"
                NativeParser[Native Parser]
                RosettaParser[Rosetta Parser]
                TrustlessValidator[Trustless Validator]
            end
        end

        subgraph "Workflow Engine"
            subgraph "Workflows"
                Backfiller[Backfiller<br/>Historical Blocks]
                Poller[Poller<br/>New Blocks]
                Streamer[Streamer<br/>Real-time]
                Monitor[Monitor<br/>Health Check]
                CrossValidator[Cross Validator]
                Replicator[Replicator]
            end
            
            subgraph "Activities"
                Extractor[Extractor]
                Loader[Loader]
                Syncer[Syncer]
                Reader[Reader]
                EventLoader[Event Loader]
            end
        end
    end

    %% Connections
    Client -->|REST/gRPC| API
    API -->|Read| BlobStorage
    API -->|Query| MetaStorage
    
    Server -->|Schedule| Temporal
    Worker -->|Execute| Temporal
    Temporal -->|Orchestrate| Workflows
    
    Workflows -->|Execute| Activities
    Activities -->|Use| ClientFactory
    ClientFactory -->|Create| Master
    ClientFactory -->|Create| Slave
    ClientFactory -->|Create| Validator
    ClientFactory -->|Create| Consensus
    
    Master -->|Fetch| BC
    Slave -->|Fetch| BC
    Validator -->|Validate| BC
    Consensus -->|Check| BC
    
    Activities -->|Parse| NativeParser
    NativeParser -->|Convert| RosettaParser
    Activities -->|Validate| TrustlessValidator
    
    Activities -->|Store Raw| BlobStorage
    Activities -->|Store Meta| MetaStorage
    BlobStorage -->|S3/GCS| AWS
    BlobStorage -->|S3/GCS| GCP
    MetaStorage -->|DynamoDB| AWS
    MetaStorage -->|Firestore| GCP
    
    Admin -->|Manual Ops| Workflows
    Admin -->|Direct Access| BlobStorage
    Admin -->|Direct Access| MetaStorage
    
    Cron -->|Scheduled| Workflows

    %% Styling
    classDef external fill:#f9f,stroke:#333,stroke-width:2px
    classDef entry fill:#bbf,stroke:#333,stroke-width:2px
    classDef storage fill:#bfb,stroke:#333,stroke-width:2px
    classDef workflow fill:#fbf,stroke:#333,stroke-width:2px
    classDef blockchain fill:#ffb,stroke:#333,stroke-width:2px
    
    class BC,Client,Temporal,AWS,GCP external
    class API,Server,Worker,Admin,Cron entry
    class BlobStorage,MetaStorage storage
    class Backfiller,Poller,Streamer,Monitor,CrossValidator,Replicator,Extractor,Loader,Syncer,Reader,EventLoader workflow
    class ClientFactory,Master,Slave,Validator,Consensus,NativeParser,RosettaParser,TrustlessValidator blockchain
```

## Component Details

### Entry Points

1. **API Server** (`cmd/api`)
   - Serves REST and gRPC endpoints
   - Provides block data, events, and metadata
   - Handles authentication and rate limiting

2. **Main Server** (`cmd/server`)
   - Manages Temporal workflows
   - Coordinates blockchain data ingestion
   - Handles workflow scheduling

3. **Worker** (`cmd/worker`)
   - Executes Temporal activities
   - Performs actual blockchain data fetching
   - Handles parsing and validation

4. **Admin CLI** (`cmd/admin`)
   - Manual workflow management
   - Direct storage operations
   - Debugging and maintenance

5. **Cron** (`cmd/cron`)
   - Scheduled maintenance tasks
   - Periodic health checks
   - Cleanup operations

### Storage Layer

1. **BlobStorage**
   - Stores compressed raw block data
   - Supports S3 (AWS) and GCS (GCP)
   - Provides presigned URLs for direct access
   - Handles block versioning

2. **MetaStorage**
   - Stores block metadata (height, hash, timestamp)
   - Maintains transaction indexes
   - Tracks events (block added/removed)
   - Supports DynamoDB (AWS) and Firestore (GCP)

### Blockchain Layer

1. **Client System**
   - **Master**: Primary endpoint with sticky sessions
   - **Slave**: Load-balanced endpoints for ingestion
   - **Validator**: Data validation endpoints
   - **Consensus**: Consensus verification endpoints

2. **Parser System**
   - **Native Parser**: Blockchain-specific parsing
   - **Rosetta Parser**: Standardized Rosetta format
   - **Trustless Validator**: Cryptographic validation

### Workflow Engine

1. **Core Workflows**
   - **Backfiller**: Historical block ingestion
   - **Poller**: Continuous new block polling
   - **Streamer**: Real-time block streaming
   - **Monitor**: System health monitoring
   - **Cross Validator**: Cross-chain validation
   - **Replicator**: Data replication

2. **Activities**
   - **Extractor**: Fetches raw blocks
   - **Loader**: Stores processed blocks
   - **Syncer**: Synchronizes blockchain state
   - **Reader**: Reads stored data
   - **Event Loader**: Processes blockchain events

## Data Flow

1. **Ingestion Flow**
   ```
   Blockchain Node → Client → Parser → Validator → Storage
   ```

2. **Query Flow**
   ```
   External Client → API Server → Storage → Response
   ```

3. **Workflow Flow**
   ```
   Temporal → Workflow → Activity → Client → Blockchain
                    ↓
                Storage
   ```

## Key Design Patterns

- **Factory Pattern**: Dynamic blockchain client/parser creation
- **Interceptor Pattern**: Request/response instrumentation
- **Module Pattern**: Clean separation of concerns
- **Dependency Injection**: Using Uber FX framework
- **Option Pattern**: Flexible configuration

## Supported Blockchains

- **EVM**: Ethereum, Polygon, BSC, Arbitrum, Optimism, Base, Fantom, Avalanche
- **Bitcoin-based**: Bitcoin, Bitcoin Cash, Dogecoin, Litecoin
- **Others**: Solana, Aptos, Tron
- **Special**: Ethereum Beacon Chain

## Scalability Features

- Horizontal scaling via distributed storage
- Multi-endpoint load balancing
- Temporal workflow orchestration
- Configurable batch processing
- Support for 1,500+ blocks/second