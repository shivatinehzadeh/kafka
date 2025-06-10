from kafka import KafkaProducer
import json
import datetime
import time

    
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(150):
    message = {'date': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'value': i}
    try:
        producer.send('test-topic', value=message)
        time.sleep(1)
    except Exception as e:
        print(f"Error sending message: {e}")

    print(f"Sent: {message}")

producer.flush()
