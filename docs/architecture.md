# ShopSphere Architecture

## 1. Architecture Overview

ShopSphere is an event-driven Azure data platform for processing e-commerce order events.

The production-style architecture consists of five major layers:

1. Event generation
2. Streaming ingestion
3. Medallion processing
4. Incremental state processing
5. Workflow orchestration

## 2. End-to-End Architecture

Python Event Generators
        |
        v
Azure Event Hubs
        |
        v
Azure Databricks
        |
        +---- Bronze Delta
        |       |
        |       v
        +---- Silver
        |       +---- orders
        |       +---- invalid_orders
        |       |
        |       v
        +---- Gold
                +---- sales / revenue metrics
                +---- product metrics
                +---- customer metrics

Silver Orders
        |
        | Delta Change Data Feed
        v
CDF Incremental Processor
        |
        | Delta MERGE
        v
current_orders

Azure Data Factory
        |
        +---- Bronze selective refresh
        +---- Silver selective refresh
        +---- Gold selective refresh
        +---- CDF/MERGE
        +---- Validation
