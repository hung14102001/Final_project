from kafka import KafkaProducer
import json
import pandas as pd

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    key_serializer=lambda m: str(m).encode('utf-8'),
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

df_Product = pd.read_csv('Product.csv')
df_Brand = pd.read_csv('Brand.csv')
df_Date = pd.read_csv('Date.csv')
df_Shop = pd.read_csv('Shop.csv')

products = df_Product.to_dict(orient='records')
for product in products:
    try:
        result = producer.send('Product', value=product)
        metadata = result.get(timeout=10)  # Wait for acknowledgement
        print(f"Message sent successfully: {metadata.topic} - Partition: {metadata.partition} - Offset: {metadata.offset}")
    except Exception as e:
        print(f"Failed to send message: {str(e)}")

brands = df_Brand.to_dict(orient='records')
for brand in brands:
    try:
        result = producer.send('Brand', value=brand)
        metadata = result.get(timeout=10)  # Wait for acknowledgement
        print(f"Message sent successfully: {metadata.topic} - Partition: {metadata.partition} - Offset: {metadata.offset}")
    except Exception as e:
        print(f"Failed to send message: {str(e)}")

shops = df_Shop.to_dict(orient='records')
for shop in shops:
    try:
        result = producer.send('Shop', value=shop)
        metadata = result.get(timeout=10)  # Wait for acknowledgement
        print(f"Message sent successfully: {metadata.topic} - Partition: {metadata.partition} - Offset: {metadata.offset}")
    except Exception as e:
        print(f"Failed to send message: {str(e)}")

dates = df_Date.to_dict(orient='records')
for date in dates:
    try:
        result = producer.send('Date', value=date)
        metadata = result.get(timeout=10)  # Wait for acknowledgement
        print(f"Message sent successfully: {metadata.topic} - Partition: {metadata.partition} - Offset: {metadata.offset}")
    except Exception as e:
        print(f"Failed to send message: {str(e)}")
producer.close()







