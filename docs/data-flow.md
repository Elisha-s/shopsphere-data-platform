```markdown
# Data Flow

## 1. Event generation

A Databricks Python wheel task generates 500 order events per run.

Each event contains:

- event metadata;
- customer and product identifiers;
- order details;
- location;
- payment method;
- timestamps.

Approximately 5% of events are assigned an event timestamp seven minutes earlier than their generation timestamp to simulate late-arriving data.

## 2. Landing

Generated events are written as newline-delimited JSON files to:

```text
/Volumes/adb_shopsphere_dev/bronze/order_events_landing