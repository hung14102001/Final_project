from airflow import DAG, task

from airflow.operators.python import PythonOperator
from confluent_kafka import Producer
from datetime import datetime
import json
import requests
import pandas as pd

params = {
    "limit": "40",
    "include": "advertisement",
    "aggregations": "2",
    "trackity_id": "c034c7d1-3e57-851c-4df6-70ae13c2b400",
    "category": "1795",
    "page": "1",
    "urlKey": "dien-thoai-smartphone",
}
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
}
ProductTable = []
BrandTable = []
# DateTable=[]
List_shop_id = []
def create_date_data(ti):
    year_begin = 2018
    year_end = datetime.now().year
    date_id = 1
    DateTable =[]
    for year in range(year_begin, year_end + 1):
        for month in range(1, 13):
            if month == 12:
                next_month = 1
                next_year = year + 1
            else:
                next_month = month + 1
                next_year = year

            days_in_month = (
                datetime(next_year, next_month, 1) - datetime(year, month, 1)
            ).days

            for day in range(1, days_in_month + 1):

                date_obj = datetime(year, month, day)
                days_ago = (datetime.now().date() - date_obj.date()).days
                if days_ago < 0:
                    break
                quarter = (month - 1) // 3 + 1
                date_dict = {
                    "dateID": date_id,
                    "date": str(date_obj.date()),
                    "day": day,
                    "month": month,
                    "year": year,
                    "month_name": str(date_obj.strftime("%B")),
                    "quarter": quarter,
                    "days_ago": days_ago,
                }
                DateTable.append(date_dict)
                date_id += 1
    ti.xcom_push(key='date_table', value = DateTable)
def extract_data(ti):

    DateTable = ti.xcom_pull(task_ids = 'create_date_data', key='date_table')
    URL = "https://tiki.vn/api/personalish/v1/blocks/listings"
    response = requests.get(URL, params=params, headers=headers, timeout=10)
    last_page = response.json()["paging"]["last_page"]
    cur_page = 17
    while cur_page < last_page + 1:
        response = requests.get(URL, params=params, headers=headers, timeout=10)
        for item in response.json()["data"]:
            res = extract_product(item)
            get_detail_product(res, DateTable)
        cur_page += 1
        params["page"] = str(cur_page)
    print(ProductTable)
    print(BrandTable)
    print(List_shop_id)
    ti.xcom_push(key='list_shop_id', value = List_shop_id)
    
    ti.xcom_push(key='brand_table', value = BrandTable)
    ti.xcom_push(key='product_table', value = ProductTable)


def extract_product(item):
    quantity_sold = item.get("quantity_sold")
    if quantity_sold is not None:
        quantity_sold_value = quantity_sold["value"]
    else:
        quantity_sold_value = 0
    product = {
        "productID": item.get("id"),
        "product_name": item.get("name"),
        "brandID": None,
        "original_price": item.get("original_price"),
        "discount": item.get("discount"),
        "price": item.get("price"),
        "discount_percent": item.get("discount_rate"),
        "rating": item.get("rating_average"),
        "review_count": item.get("review_count"),
        "quantity_sold": quantity_sold_value,
        "shopID": item.get("seller_id"),
        "dateID": None
    }
    return product

def get_detail_product(product, DateTable):

    # print(DateTable)
    id = product["productID"]
    response = requests.get(
        f"https://tiki.vn/api/v2/products/{id}",
        headers=headers,
        timeout=10,
        params=params,
    )
    if response.status_code == 200:
        text = response.content
        # print(text)
        data = json.loads(text)

        day_ago_created = None
        day_ago_created = data["day_ago_created"]

        for date_dict in DateTable:
            if date_dict["days_ago"] == day_ago_created:
                product["dateID"] = date_dict["dateID"]

        brand_ = {}
        brandID = data["brand"]["id"]
        brand_name = data["brand"]["name"]
        brand_ = {
            "brandID": brandID,
            "brand_name": brand_name,
        }
        product["brandID"] = brandID
        if product not in ProductTable:
            ProductTable.append(product)

        if brand_ not in BrandTable:
            BrandTable.append(brand_)

        
        current_seller = data["current_seller"]
        if current_seller is not None:
            shopID = data["current_seller"]["id"]

            if shopID not in List_shop_id:
                List_shop_id.append(shopID)


def create_shop_table(ti):
    List_shops = ti.xcom_pull(task_ids = 'extract_data', key='list_shop_id')
    if List_shops:
        ShopTable = []
        for shopID in List_shops:
            shopInfo = requests.get(
                f"https://tiki.vn/api/shopping/v2/widgets/seller?seller_id={shopID}",
                headers=headers,
                timeout=10,
            )
            dataShop = json.loads(shopInfo.content).get("data")
            print(shopInfo.content)
            avg_rating_point = dataShop["seller"]["avg_rating_point"]
            days_since_joined = dataShop["seller"]["days_since_joined"]
            review_count = dataShop["seller"]["review_count"]
            total_follower = dataShop["seller"]["total_follower"]

            shopName = dataShop["seller"]["name"]
            shop_ = {}
            shop_ = {
                "Shop_ID": shopID,
                "shopName": shopName,
                "avg_rating_point": avg_rating_point,
                "days_since_joined": days_since_joined,
                "review_count": review_count,
                "total_follower": total_follower,
            }
            print(shop_)
            ShopTable.append(shop_)
        print(ShopTable)
        ti.xcom_push(key='shop_table_ti', value = ShopTable)
    else:
        print(List_shops)
    
def load_data(ti):
    from kafka import KafkaProducer

    producer = Producer({'bootstrap.servers': '192.168.1.231:9092)'})
    DateTable = ti.xcom_pull(task_ids = 'create_date_data', key='date_table')
    print(DateTable)
    ProductTable = ti.xcom_pull(task_ids = 'extract_data', key='product_table')
    BrandTable = ti.xcom_pull(task_ids = 'extract_data', key='brand_table')
    ShopTable = ti.xcom_pull(task_ids = 'create_shop_data', key='shop_table_ti')
    print(ProductTable)
    print(BrandTable)
    print(ShopTable)

    producer = KafkaProducer(
        bootstrap_servers=['192.168.1.231:9092'],
        key_serializer=lambda m: str(m).encode('utf-8'),
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )
    ProductTable = sorted(ProductTable, key=lambda x: x["productID"])

    BrandTable = sorted(BrandTable, key=lambda x: x["brandID"])

    ShopTable = sorted(ShopTable, key=lambda x: x["Shop_ID"])
    df_Product = pd.DataFrame(data=ProductTable)
    df_Brand = pd.DataFrame(data=BrandTable)
    df_Shop = pd.DataFrame(data=ShopTable)
    df_Date = pd.DataFrame(data=DateTable)

    # Chuyển đổi DataFrame thành danh sách các bản ghi đơn
    products = df_Product.to_dict(orient='records')
    for product in products:
    # Send the record as JSON
        try:
            result = producer.send('topicProduct', value=product)
            metadata = result.get(timeout=10)  # Wait for acknowledgement
            print(f"Message sent successfully: {metadata.topic} - Partition: {metadata.partition} - Offset: {metadata.offset}")
        except Exception as e:
            print(f"Failed to send message: {str(e)}")

    brands = df_Brand.to_dict(orient='records')
    for brand in brands:
    # Send the record as JSON
        try:
            result = producer.send('topicBrand', value=brand)
            metadata = result.get(timeout=10)  # Wait for acknowledgement
            print(f"Message sent successfully: {metadata.topic} - Partition: {metadata.partition} - Offset: {metadata.offset}")
        except Exception as e:
            print(f"Failed to send message: {str(e)}")

    shops = df_Shop.to_dict(orient='records')
    for shop in shops:
    # Send the record as JSON
        try:
            result = producer.send('topicShop', value=shop)
            metadata = result.get(timeout=10)  # Wait for acknowledgement
            print(f"Message sent successfully: {metadata.topic} - Partition: {metadata.partition} - Offset: {metadata.offset}")
        except Exception as e:
            print(f"Failed to send message: {str(e)}")

    dates = df_Date.to_dict(orient='records')
    for date in dates:
    # Send the record as JSON
        try:
            result = producer.send('topicDate', value=date)
            metadata = result.get(timeout=10)  # Wait for acknowledgement
            print(f"Message sent successfully: {metadata.topic} - Partition: {metadata.partition} - Offset: {metadata.offset}")
        except Exception as e:
            print(f"Failed to send message: {str(e)}")
    producer.close()

default_args = {
    'owner': 'hunglt1410',
    'start_date': datetime(2023, 5, 15),
    'catchup': False
}

with DAG(
    'etl_tiki.vn',
    default_args=default_args,
    description='Extracts data from Tiki.vn API and load to kafka',
    schedule_interval='@daily',
) as dag:
    task1 = PythonOperator(
        task_id='create_date_data',
        python_callable=create_date_data,
    )
    task2 = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )
    task3 = PythonOperator(
        task_id='create_shop_data',
        python_callable=create_shop_table,
        # dag=dag
    )
    task4 = PythonOperator(
        task_id='load_data_to_kafka',
        python_callable=load_data,
        
        
        # dag=dag
    )


task1 >> task2 >> task3 >> task4

