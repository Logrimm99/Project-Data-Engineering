from kafka import KafkaConsumer
import psycopg2
import json
import time

# Kafka Consumer Setup
while True:
    print('Trying to connect to Kafka', flush=True)
    try:
        consumer = KafkaConsumer(
            "processed-data",
            bootstrap_servers='kafka:9092',
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='PLAIN',
            sasl_plain_username='consumer',
            sasl_plain_password='consumer-secret',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        break
    except Exception as e:
        print(f"Kafka not ready, retrying in 5 seconds: {e}", flush=True)
        time.sleep(5)

print('Kafka connection established', flush=True)

# PostgreSQL Connection Setup
conn = psycopg2.connect(
    dbname="mydb",
    user="user",
    password="password",
    host="db",
    port="5432",
)
cursor = conn.cursor()


def ensure_table_exists(data):
    """
    Dynamically create or update the table structure to match the incoming data.
    """
    # Fetch existing columns in the table
    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'aggregated_data';
    """)
    existing_columns = {row[0] for row in cursor.fetchall()}

    # Add missing columns dynamically
    for key in data.keys():
        if key not in existing_columns:
            cursor.execute(f"""
                ALTER TABLE aggregated_data ADD COLUMN {key} TEXT;
            """)
            print(f"Added column: {key}", flush=True)

    conn.commit()


# Create the base table if it doesn't exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS aggregated_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")
conn.commit()

# Process messages from Kafka
for message in consumer:
    # Decode and parse the Kafka message
    data = message.value

    # Ensure the table matches the incoming data structure
    ensure_table_exists(data)

    # Dynamically prepare the INSERT query
    columns = ', '.join(data.keys())
    placeholders = ', '.join(['%s'] * len(data))
    values = tuple(data.values())

    print('Data: ', data)
    print('Columns: ', columns)
    print('Placeholders: ', placeholders)

    query = f"""
        INSERT INTO aggregated_data ({columns})
        VALUES ({placeholders});
    """
    cursor.execute(query, values)
    conn.commit()

    print(f"Stored in DB: {data}", flush=True)
