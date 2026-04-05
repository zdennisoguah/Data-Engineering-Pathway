# Databricks notebook source
# Install Event Hubs Spark connector
# Run this in its own cell first
% pip install azure-eventhub

# COMMAND ----------

import json
from azure.eventhub import EventHubConsumerClient

CONNECTION_STR = "your_actual_connection_string_here"  # from Azure Portal → Event Hubs → Shared access policies
EVENTHUB_NAME  = "trip-events"
CONSUMER_GROUP = "$Default"

events_collected = []

def on_event(partition_context, event):
    # Guard against empty heartbeat events
    if event is None:
        return

    try:
        data = json.loads(event.body_as_str())
        events_collected.append(data)
        partition_context.update_checkpoint()
        print(f"Received: {data.get('event_type')} — {data.get('trip_id')}")
    except Exception as e:
        print(f"Failed to parse event: {e}")

def on_error(partition_context, error):
    print(f"Partition error: {error}")

client = EventHubConsumerClient.from_connection_string(
    CONNECTION_STR,
    consumer_group=CONSUMER_GROUP,
    eventhub_name=EVENTHUB_NAME
)

print("Listening for events (15 seconds)...")
with client:
    client.receive(
        on_event=on_event,
        on_error=on_error,
        starting_position="-1",
        max_wait_time=15
    )

print(f"\nTotal events received: {len(events_collected)}")
if events_collected:
    print("Sample event:")
    print(json.dumps(events_collected[0], indent=2))