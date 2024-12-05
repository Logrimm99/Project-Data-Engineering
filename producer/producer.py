# Group Tweets by Day
from kafka import KafkaProducer
import csv
import time
import json
from collections import defaultdict
from datetime import datetime

while True:
    try:
        # producer = KafkaProducer(bootstrap_servers='kafka:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='PLAIN',
            sasl_plain_username='producer',
            sasl_plain_password='producer-secret',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        break
    except Exception as e:
        print(f"Kafka not ready, retrying in 5 seconds: {e}", flush=True)
        time.sleep(5)

topic = "raw-data"
filePath = 'TWITTER_REVIEWS.csv'

# Read and sort tweets by timestamp
tweets = []

with open(filePath, 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Parse timestamp and add it to the data
        row['review_timestamp'] = datetime.strptime(row['review_timestamp'], '%Y-%m-%d %H:%M:%S')
        tweets.append(row)

# Sort tweets by timestamp (earliest first)
tweets.sort(key=lambda x: x['review_timestamp'])

# Group tweets by day
tweets_by_day = defaultdict(list)
for tweet in tweets:
    tweet_date = tweet['review_timestamp'].strftime('%Y-%m-%d')
    tweets_by_day[tweet_date].append(tweet)

# Send grouped data to Kafka
for day, tweets_group in tweets_by_day.items():
    # Convert timestamp back to string for JSON serialization
    for tweet in tweets_group:
        tweet['review_timestamp'] = tweet['review_timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        # print('Tweet:', tweet)

    payload = {"date": day, "tweets": tweets_group}
    producer.send(topic, value=payload)
    print('Sent payload: ', payload.keys())
    print(f"Sent tweets for {day}: {len(tweets_group)} tweets", flush=True)
    time.sleep(0.1)

print(f"All grouped data points sent!", flush=True)

