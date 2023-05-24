CREATE TABLE "DimBrand" (
    brandID numeric(10) NOT NULL PRIMARY KEY ,
    brand_name varchar(20) NOT NULL );


CREATE TABLE "DimShop"(
    shopID integer NOT NULL PRIMARY KEY, 
    shop_name varchar(40) NOT NULL, 
    avg_rating_point DECIMAL(8,4)  NOT NULL, 
    days_since_joined integer, 
    review_count integer,  
    total_follower integer);


CREATE TABLE "DimDate" (
    dateID integer PRIMARY KEY ,
    date DATE NOT NULL,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    month_name VARCHAR(12),
    quarter smallint,
    days_ago INTEGER
);


CREATE TABLE "FactSaleProduct" (
    productID NUMERIC(15) NOT NULL PRIMARY KEY, 

    product_name varchar(250) NOT NULL, 
    brandID NUMERIC(10) NOT NULL, 
    original_price integer, 
    discount integer, 
    price integer, 
    discount_percent varchar(10), 
    rating varchar(10), 
    review_count smallint,
    quantity_sold smallint, 
    shopID integer,
    dateID integer,

    FOREIGN KEY (brandID) REFERENCES "DimBrand" (brandID),
    FOREIGN KEY (shopID) REFERENCES "DimShop" (shopID),
    FOREIGN KEY (dateID) REFERENCES "DimDate" (dateID)
);









