import csv
import random
from datetime import datetime, timedelta

random.seed(99)

drivers = [
    ("D101","Emeka Eze","sedan"),
    ("D102","Amara Nwosu","sedan"),
    ("D103","Tunde Alabi","bike"),
    ("D104","Funke Osei","suv"),
    ("D105","Chukwuemeka Okonkwo","sedan"),
    ("D106","Ngozi Adeyemi","sedan"),
    ("D107","Bello Musa","suv"),
    ("D108","Kemi Adeleke","sedan"),
    ("D109","Chidi Okafor","sedan"),
    ("D110","Sade Williams","sedan"),
]

cities = {
    "Lagos": ["Victoria Island","Lekki Phase 1","Ikeja","Surulere","Yaba","Ajah","Gbagada"],
    "Abuja": ["Wuse 2","Maitama","Garki","Asokoro","Gwarinpa"],
    "Port Harcourt": ["GRA Phase 2","GRA Phase 3","Rumuola","Trans Amadi"],
    "Kano": ["Nassarawa","Fagge","Sabon Gari","Bompai"],
    "Ibadan": ["Bodija","Ring Road","Dugbe","Agodi"],
}

header = [
    "trip_id","driver_id","driver_name","rider_id","pickup_city",
    "pickup_location","dropoff_city","dropoff_location","request_time",
    "pickup_time","dropoff_time","distance_km","fare_amount",
    "surge_multiplier","payment_type","trip_status","rating","vehicle_type"
]

rows = []
base_date = datetime(2024, 2, 26, 7, 0, 0)

for i in range(1, 51):
    trip_id   = f"T{500 + i}"
    driver    = random.choice(drivers)
    driver_id, driver_name, vehicle = driver
    city      = random.choice(list(cities.keys()))
    locs      = cities[city]
    pickup    = random.choice(locs)
    dropoff   = random.choice([l for l in locs if l != pickup])
    rider_id  = f"R{random.randint(200, 400)}"

    minutes_offset = random.randint(0, 1380)
    request_dt     = base_date + timedelta(minutes=minutes_offset)
    pickup_dt      = request_dt + timedelta(minutes=random.randint(3, 12))
    fare           = round(random.uniform(600, 4500) / 10) * 10
    surge          = random.choice([1.0, 1.0, 1.0, 1.2, 1.5])
    fare           = round(fare * surge, 2)
    distance       = round(fare / random.uniform(130, 170), 1)
    trip_mins      = max(10, int(distance * random.uniform(3, 5)))
    dropoff_dt     = pickup_dt + timedelta(minutes=trip_mins)
    status         = random.choice(["completed","completed","completed","completed","cancelled"])
    payment        = random.choice(["cash","card","wallet"])
    rating         = round(random.uniform(3.2, 5.0), 1) if status == "completed" else None

    rows.append([
        trip_id, driver_id, driver_name, rider_id, city, pickup,
        city, dropoff,
        request_dt.strftime("%Y-%m-%d %H:%M:%S"),
        pickup_dt.strftime("%Y-%m-%d %H:%M:%S"),
        dropoff_dt.strftime("%Y-%m-%d %H:%M:%S") if status == "completed" else "",
        distance, fare, surge, payment, status,
        rating if rating else "", vehicle
    ])

output_path = "/tmp/new_trips_2024_02_26.csv"
with open(output_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(rows)

# Print a summary of what was generated
completed = sum(1 for r in rows if r[15] == "completed")
cancelled = sum(1 for r in rows if r[15] == "cancelled")
cities_covered = set(r[4] for r in rows)

print(f"New batch generated: {len(rows)} trips")
print(f"  Completed : {completed}")
print(f"  Cancelled : {cancelled}")
print(f"  Cities    : {', '.join(sorted(cities_covered))}")
print(f"  Trip IDs  : T501 – T550")
print(f"  Date      : 2024-02-26")
print(f"  Saved to  : {output_path}")