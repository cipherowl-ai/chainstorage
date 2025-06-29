# ChainStorage Component Diagram

## Overview

ChainStorage is a distributed blockchain data storage and processing system that continuously replicates blockchain data and serves it through APIs.

## High-Level Architecture (Simplified)

```mermaid
graph LR
    %% External
    BC[Blockchain<br/>Nodes] 
    ExtClients[External<br/>Clients]
    
    %% Core Components
    API[API Server]
    WF[Workflow<br/>Engine]
    BCC[Blockchain<br/>Clients]
    P[Parsers]
    
    %% Storage
    BS[Blob<br/>Storage]
    MS[Meta<br/>Storage]
    
    %% Flow
    ExtClients -->|REST/gRPC| API
    API --> BS
    API --> MS
    
    WF -->|Fetch| BCC
    BCC -->|Raw Data| BC
    BCC --> P
    P --> BS
    P --> MS
    
    %% Styling
    style BC fill:#e3f2fd,stroke:#1565c0,stroke-width:2px
    style ExtClients fill:#e3f2fd,stroke:#1565c0,stroke-width:2px
    style API fill:#f3e5f5,stroke:#6a1b9a,stroke-width:2px
    style WF fill:#fff8e1,stroke:#f57f17,stroke-width:2px
    style BCC fill:#fce4ec,stroke:#c2185b,stroke-width:2px
    style P fill:#fce4ec,stroke:#c2185b,stroke-width:2px
    style BS fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
    style MS fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
```

## Detailed Architecture

```mermaid
graph TB
    %% External Systems
    subgraph EXT["🌐 External Systems"]
        BC[Blockchain Nodes<br/>Bitcoin/Ethereum/Solana/etc]
        Client[External Clients<br/>SDK/REST/gRPC]
        Temporal[Temporal Server]
        AWS[AWS Services<br/>S3/DynamoDB/SQS]
        GCP[GCP Services<br/>GCS/Firestore]
    end

    %% Entry Points
    subgraph ENTRY["🚀 Entry Points (cmd/)"]
        API[API Server<br/>REST/gRPC]
        Server[Main Server<br/>Workflow Manager]
        Worker[Worker<br/>Activity Executor]
        Admin[Admin CLI]
        Cron[Cron Jobs]
    end

    %% Storage Layer
    subgraph STORAGE["💾 Storage Layer"]
        BlobStorage[BlobStorage<br/>Raw Block Data]
        MetaStorage[MetaStorage<br/>Metadata & Indexes]
    end

    %% Blockchain Layer
    subgraph BLOCKCHAIN["⛓️ Blockchain Layer"]
        ClientFactory[Client Factory]
        subgraph CLIENTS["Multi-Endpoint Clients"]
            Master[Master Client<br/>Sticky Sessions]
            Slave[Slave Client<br/>Load Balanced]
            Validator[Validator Client]
            Consensus[Consensus Client]
        end
        
        subgraph PARSERS["Parsers"]
            NativeParser[Native Parser]
            RosettaParser[Rosetta Parser]
            TrustlessValidator[Trustless Validator]
        end
    end

    %% Workflow Engine
    subgraph WORKFLOW["⚙️ Workflow Engine"]
        subgraph WORKFLOWS["Workflows"]
            Backfiller[Backfiller<br/>Historical Blocks]
            Poller[Poller<br/>New Blocks]
            Streamer[Streamer<br/>Real-time]
            Monitor[Monitor<br/>Health Check]
            CrossValidator[Cross Validator]
            Replicator[Replicator]
        end
        
        subgraph ACTIVITIES["Activities"]
            Extractor[Extractor]
            Loader[Loader]
            Syncer[Syncer]
            Reader[Reader]
            EventLoader[Event Loader]
        end
    end

    %% Connections
    Client -->|REST/gRPC| API
    API -->|Read| BlobStorage
    API -->|Query| MetaStorage
    
    Server -->|Schedule| Temporal
    Worker -->|Execute| Temporal
    Temporal -->|Orchestrate| WORKFLOWS
    
    WORKFLOWS -->|Execute| ACTIVITIES
    ACTIVITIES -->|Use| ClientFactory
    ClientFactory -->|Create| Master
    ClientFactory -->|Create| Slave
    ClientFactory -->|Create| Validator
    ClientFactory -->|Create| Consensus
    
    Master -->|Fetch| BC
    Slave -->|Fetch| BC
    Validator -->|Validate| BC
    Consensus -->|Check| BC
    
    ACTIVITIES -->|Parse| NativeParser
    NativeParser -->|Convert| RosettaParser
    ACTIVITIES -->|Validate| TrustlessValidator
    
    ACTIVITIES -->|Store Raw| BlobStorage
    ACTIVITIES -->|Store Meta| MetaStorage
    BlobStorage -->|S3/GCS| AWS
    BlobStorage -->|S3/GCS| GCP
    MetaStorage -->|DynamoDB| AWS
    MetaStorage -->|Firestore| GCP
    
    Admin -->|Manual Ops| WORKFLOWS
    Admin -->|Direct Access| BlobStorage
    Admin -->|Direct Access| MetaStorage
    
    Cron -->|Scheduled| WORKFLOWS

    %% Styling with better colors
    classDef external fill:#e1f5fe,stroke:#01579b,stroke-width:2px,color:#01579b
    classDef entry fill:#f3e5f5,stroke:#4a148c,stroke-width:2px,color:#4a148c
    classDef storage fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px,color:#1b5e20
    classDef workflow fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#e65100
    classDef blockchain fill:#fce4ec,stroke:#880e4f,stroke-width:2px,color:#880e4f
    classDef subgraphStyle fill:#f5f5f5,stroke:#424242,stroke-width:1px
    
    class BC,Client,Temporal,AWS,GCP external
    class API,Server,Worker,Admin,Cron entry
    class BlobStorage,MetaStorage storage
    class Backfiller,Poller,Streamer,Monitor,CrossValidator,Replicator,Extractor,Loader,Syncer,Reader,EventLoader workflow
    class ClientFactory,Master,Slave,Validator,Consensus,NativeParser,RosettaParser,TrustlessValidator blockchain
    class EXT,ENTRY,STORAGE,BLOCKCHAIN,WORKFLOW,WORKFLOWS,ACTIVITIES,CLIENTS,PARSERS subgraphStyle
```

## Component Breakdown

### Workflow Engine Detail

```mermaid
graph TB
    T[Temporal Server]
    
    subgraph Workflows
        BF[Backfiller]
        PL[Poller]
        ST[Streamer]
        MN[Monitor]
        CV[Cross Validator]
        RP[Replicator]
    end
    
    subgraph Activities
        EX[Extractor]
        LD[Loader]
        SY[Syncer]
        RD[Reader]
        EL[Event Loader]
    end
    
    T --> Workflows
    Workflows --> Activities
    
    style T fill:#e3f2fd,stroke:#1565c0,stroke-width:2px
    style Workflows fill:#fff8e1,stroke:#f57f17,stroke-width:2px
    style Activities fill:#fff8e1,stroke:#f57f17,stroke-width:2px
```

### Blockchain Client Architecture

```mermaid
graph LR
    CF[Client Factory]
    
    subgraph Endpoints
        M[Master<br/>Sticky Sessions]
        S[Slave<br/>Load Balanced]
        V[Validator]
        C[Consensus]
    end
    
    subgraph Parsers
        NP[Native Parser]
        RP[Rosetta Parser]
        TV[Trustless Validator]
    end
    
    CF --> Endpoints
    Endpoints --> Parsers
    
    style CF fill:#fce4ec,stroke:#c2185b,stroke-width:2px
    style Endpoints fill:#fce4ec,stroke:#c2185b,stroke-width:2px
    style Parsers fill:#fce4ec,stroke:#c2185b,stroke-width:2px
```

### Storage Architecture

```mermaid
graph TB
    subgraph BlobStorage
        S3[AWS S3]
        GCS[Google Cloud Storage]
    end
    
    subgraph MetaStorage
        DDB[DynamoDB]
        FS[Firestore]
    end
    
    BS[Blob Storage<br/>Interface] --> S3
    BS --> GCS
    
    MS[Meta Storage<br/>Interface] --> DDB
    MS --> FS
    
    style BlobStorage fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
    style MetaStorage fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
    style BS fill:#c8e6c9,stroke:#1b5e20,stroke-width:2px
    style MS fill:#c8e6c9,stroke:#1b5e20,stroke-width:2px
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