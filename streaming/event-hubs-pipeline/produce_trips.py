import asyncio
import json
import random
from datetime import datetime
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

# Replace with your connection string and event hub name
CONNECTION_STR = "your_actual_connection_string_here"  # from Azure Portal → Event Hubs → Shared access policies
EVENTHUB_NAME = "trip-events"

# Simulated trip event generator
DRIVERS = ["D101", "D102", "D103", "D104", "D105"]
RIDERS = ["R201", "R202", "R203", "R204", "R205"]
CITIES = ["Lagos", "Abuja", "Port Harcourt"]
STATUSES = ["trip_requested", "driver_assigned", "trip_started", "trip_completed"]

def generate_trip_event():
    return {
        "event_id": f"EVT{random.randint(1000, 9999)}",
        "event_type": random.choice(STATUSES),
        "trip_id": f"T{random.randint(500, 999)}",
        "driver_id": random.choice(DRIVERS),
        "rider_id": random.choice(RIDERS),
        "city": random.choice(CITIES),
        "fare_amount": round(random.uniform(500, 5000), 2),
        "timestamp": datetime.now().isoformat()
    }

async def produce_events(count=20):
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENTHUB_NAME
    )
    async with producer:
        batch = await producer.create_batch()
        for i in range(count):
            event = generate_trip_event()
            batch.add(EventData(json.dumps(event)))
            print(f"Queued event {i+1}: {event['event_type']} — {event['trip_id']}")

        await producer.send_batch(batch)
        print(f"\n{count} events sent to Event Hub: {EVENTHUB_NAME}")

asyncio.run(produce_events(20))