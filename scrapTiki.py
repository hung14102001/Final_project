import time
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer
from datetime import datetime
import json
from kafka import KafkaConsumer,KafkaProducer
import requests
import pandas as pd

def create_date_table():
    year_begin = 2018
    year_end = datetime.now().year
    date_id = 1

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
    # print(DateTable)

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

def get_detail_product(product):
    time.sleep(0.5)
    id = product["productID"]
    response = requests.get(
        f"https://tiki.vn/api/v2/products/{id}",
        headers=headers,
        timeout=10,
        params=params,
    )
    if response.status_code == 200:
        text = response.content
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
def create_shop_table():
    for shopID in List_shop_id:
        shopInfo = requests.get(
            f"https://tiki.vn/api/shopping/v2/widgets/seller?seller_id={shopID}",
            headers=headers,
            timeout=10,
        )
        dataShop = json.loads(shopInfo.content).get("data")
        # print(shopInfo.content)
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
        # print(shop_)
        ShopTable.append(shop_)
    print(ShopTable)


    
def load_data(ProductTable,ShopTable,BrandTable,DateTable):

    
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

    df_Product.to_csv("Product.csv", index = False, encoding="UTF-8")
    df_Brand.to_csv("Brand.csv", index = False, encoding="UTF-8")
    df_Shop.to_csv("Shop.csv", index = False, encoding="UTF-8")
    df_Date.to_csv("Date.csv", index = False, encoding="UTF-8")
    # Chuyển đổi DataFrame thành danh sách các bản ghi đơn


    products = df_Product.to_dict(orient='records')
    for product in products:
        producer.send('testProduct', value=product)

    brands = df_Brand.to_dict(orient='records')
    for brand in brands:
        producer.send('testBrand', value=brand)

    shops = df_Shop.to_dict(orient='records')
    for shop in shops:
        producer.send('testShop', value=shop)

    dates = df_Date.to_dict(orient='records')
    for date in dates:
        producer.send('testDate', value=date)

    producer.close()
if __name__ == "__main__":

    URL = "https://tiki.vn/api/personalish/v1/blocks/listings"
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
    ShopTable = []
    DateTable = []
    List_shop_id = []
    create_date_table()
    response = requests.get(URL, params=params, headers=headers, timeout=10)
    last_page = response.json()["paging"]["last_page"]
    print(last_page)
    cur_page = 1
    while cur_page < last_page + 1:
        print(cur_page)
        response = requests.get(URL, params=params, headers=headers, timeout=10)
        for item in response.json()["data"]:
            res = extract_product(item)
            get_detail_product(res)
        cur_page += 1
        params["page"] = str(cur_page)
    create_shop_table()
    # print(ProductTable)
    # print(ShopTable)
    # print(BrandTable)
    load_data(ProductTable,ShopTable,BrandTable,DateTable)
