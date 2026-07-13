import json
import random
import time
import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from kafka.admin import KafkaAdminClient, NewTopic

CITIES = ["Delhi", "Mumbai", "Bengaluru", "Hyderabad", "Chennai", "Pune"]
STATUSES = ["Shipment Created", "Vehicle Assigned", "In Transit", "Out for Delivery", "Delivered"]
PRIORITIES = ["High", "Medium", "Low"]

def create_topic_if_not_exists(bootstrap_servers, topic_name):
    print("Checking/creating topic...")
    while True:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id='admin-client'
            )
            break
        except Exception as e:
            print(f"Waiting for Kafka broker to be available: {e}")
            time.sleep(2)
            
    try:
        topic_list = admin_client.list_topics()
        if topic_name not in topic_list:
            print(f"Creating topic {topic_name}...")
            topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            print(f"Topic {topic_name} created successfully.")
        else:
            print(f"Topic {topic_name} already exists.")
    except Exception as e:
        print(f"Error checking/creating topic: {e}")
    finally:
        admin_client.close()

def run_producer():
    bootstrap_servers = 'localhost:9092'
    topic_name = 'shipment-events'
    
    create_topic_if_not_exists(bootstrap_servers, topic_name)
    
    print("Connecting producer to Kafka...")
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Producer connected to Kafka.")
            break
        except NoBrokersAvailable:
            print("Kafka broker not available, retrying in 2 seconds...")
            time.sleep(2)
        except Exception as e:
            print(f"Unexpected error connecting producer: {e}, retrying...")
            time.sleep(2)

    shipment_id_counter = 1000
    
    # Active shipments list: list of dicts {shipment_id, origin, destination, vehicle_id, weight, priority, revenue, status_idx}
    active_shipments = []
    
    print("Generating events...")
    try:
        while True:
            # Randomly decide whether to create a new shipment or progress an existing one
            if not active_shipments or random.random() < 0.3:
                shipment_id_counter += 1
                shipment_id = f"SH-{shipment_id_counter}"
                origin = random.choice(CITIES)
                destination = random.choice(list(set(CITIES) - {origin}))
                vehicle_id = f"V-{random.randint(100, 300)}"
                weight = round(random.uniform(10.0, 1500.0), 2)
                priority = random.choices(PRIORITIES, weights=[0.2, 0.5, 0.3], k=1)[0]
                base_rate = 15.0 if priority == "High" else 10.0
                revenue = round(weight * base_rate, 2)
                
                shipment = {
                    "shipment_id": shipment_id,
                    "origin": origin,
                    "destination": destination,
                    "vehicle_id": vehicle_id,
                    "weight": weight,
                    "priority": priority,
                    "revenue": revenue,
                    "status_idx": 0
                }
                
                if len(active_shipments) < 100:
                    active_shipments.append(shipment)
            else:
                # Progress an existing shipment
                shipment = random.choice(active_shipments)
                shipment["status_idx"] += 1
                
                # If it has reached 'Delivered', remove it from active shipments after sending
                if shipment["status_idx"] >= len(STATUSES) - 1:
                    active_shipments.remove(shipment)

            # Build payload
            payload = {
                "shipment_id": shipment["shipment_id"],
                "origin": shipment["origin"],
                "destination": shipment["destination"],
                "vehicle_id": shipment["vehicle_id"],
                "weight": shipment["weight"],
                "priority": shipment["priority"],
                "revenue": shipment["revenue"],
                "status": STATUSES[shipment["status_idx"]],
                "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Send message
            producer.send(topic_name, payload)
            print(f"Sent: {payload['shipment_id']} - {payload['status']} ({payload['priority']})")
            
            # Wait random time between 0.5 and 1.5 seconds
            time.sleep(random.uniform(0.5, 1.5))
            
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()
