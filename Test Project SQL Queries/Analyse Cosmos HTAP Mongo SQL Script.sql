-- https://docs.microsoft.com/en-us/learn/modules/query-azure-cosmos-db-with-sql-serverless-for-azure-synapse-analytics/

CREATE DATABASE SynapseLinkDB
GO;

USE SynapseLinkDB
GO;


SELECT TOP(10) * 
FROM OPENROWSET('CosmosDB',
                'Account=mycosmosdbsynapselink;Database=CustomerProfile;Key=SlkV9QjUKWfenBKpO0YaagM8E29lxMN9vNfAp0EfRWg61ejXXf9apRNRSxW8E7ux5A2hZBM0GQv7qHHsBSGGow=='
                ,CustomerProfileHTAP) AS Customer
WHERE customerid='647';


CREATE VIEW Customers
AS
SELECT *
    FROM OPENROWSET('CosmosDB',
                'Account=mycosmosdbsynapselink;Database=CustomerProfile;Key=SlkV9QjUKWfenBKpO0YaagM8E29lxMN9vNfAp0EfRWg61ejXXf9apRNRSxW8E7ux5A2hZBM0GQv7qHHsBSGGow=='
                ,CustomerProfileHTAP) 
            WITH 
            (
                    CustomerId varchar(max) '$.id',
                    Title varchar(max) '$.title',
                    FirstName varchar(max) '$.First Name',
                    LastName varchar(max) '$.Last Name',
                    EmailAddress varchar(max) '$.email',
                    PhoneNumber varchar(max) '$.Phone',
                    AddressLine1 varchar(max) '$.address.addressLine1',
                    AddressLine2 varchar(max) '$.address.addressLine2',
                    City varchar(max) '$.address.city',
                    Country varchar(max) '$.address.country',
                    ZipCode varchar(max) '$.address.zipCode'
            )
                    AS Customers;


OR


CREATE VIEW Customers
AS
SELECT 
CustomerId, CustomerNumId, Title,
CONCAT(C."First Name", C.FirstName) as FirstName, 
CONCAT(c.LastName,c."Last Name")  as LastName,
EmailAddress, PhoneNumber, AddressLine1, AddressLine2, City, Country, ZipCode
FROM 
    (
    SELECT *
    FROM OPENROWSET('CosmosDB',
                'Account=mycosmosdbsynapselink;Database=CustomerProfile;Key=SlkV9QjUKWfenBKpO0YaagM8E29lxMN9vNfAp0EfRWg61ejXXf9apRNRSxW8E7ux5A2hZBM0GQv7qHHsBSGGow=='
                ,CustomerProfileHTAP) 
    WITH 
            (
                    CustomerId varchar(max) '$.id',
                    CustomerNumId varchar(max) '$.customerid',
                    Title varchar(max) '$.title',
                    "First Name" varchar(max) '$.First Name',
                    FirstName varchar(max) '$.FirstName',
                    LastName varchar(max) '$.LastName',
                    "Last Name" varchar(max) '$.Last Name',
                    EmailAddress varchar(max) '$.email',
                    PhoneNumber varchar(max) '$.Phone',
                    AddressLine1 varchar(max) '$.address.addressLine1',
                    AddressLine2 varchar(max) '$.address.addressLine2',
                    City varchar(max) '$.address.city',
                    Country varchar(max) '$.address.country',
                    ZipCode varchar(max) '$.address.zipCode'
            )
    AS Customers
    -- WHERE customerid='647'
    ) C;



SELECT TOP(10) * FROM Customers;



SELECT top 10 *
    FROM OPENROWSET('CosmosDB',
                    'Account=adventureworks-mongodb;Database=AdventureWorks;Key=v2mtZ85W0AMCv1ZrY7j…4g==',
                    SalesOrder) As SalesOrders;



CREATE VIEW SalesOrders
AS
SELECT  SalesOrderId, SalesOrders.customerId CustomerId, 
        CONVERT(date,orderDate) as OrderDate, CONVERT(date,shipdate) AS ShipDate,
        Customers.Country, Customers.City
    FROM OPENROWSET('CosmosDB',
                    'Account=adventureworks-mongodb;Database=AdventureWorks;Key=v2mtZ85W0AMCv1Zr…D3v4g==',
                    SalesOrder)
                        WITH 
                        (
                            SalesOrderId varchar(max) '$._id.string', 
                            customerId  varchar(max) '$.customerId.string',
                            orderDate varchar(max) '$.orderDate.string',
                            shipDate varchar(max) '$.shipDate.string'
                        )                  
                        As SalesOrders
            INNER JOIN Customers
                On SalesOrders.customerId = Customers.CustomerId;



SELECT top 10 * FROM SalesOrders;


SELECT TOP(10) SalesOrderId, details
   FROM OPENROWSET('CosmosDB',
                'Account=adventureworks-mongodb;Database=AdventureWorks;Key=v2mtZ85W0AMCv1ZrY7jMUOWpfBTi1BrUz0Y3Rwmvj9SXSSIKDU7EQVu5kdEMcwAQfvJBnmHSMyxy50c3gD3v4g==',
                SalesOrder)
                WITH 
                (   SalesOrderId varchar(max) '$._id', 
                    details varchar(max) '$.details'
                )  As SalesOrders;


SELECT TOP(10) SalesOrderId, details
   FROM OPENROWSET('CosmosDB',
                'Account=adventureworks-mongodb;Database=AdventureWorks;Key=v2mtZ85W0AMCv1ZrY7jMUOWpfBTi1BrUz0Y3Rwmvj9SXSSIKDU7EQVu5kdEMcwAQfvJBnmHSMyxy50c3gD3v4g==',
                SalesOrder)
                  WITH 
                    (   SalesOrderId varchar(max) '$._id.string', 
                        details varchar(max) '$.details.array'
                    )  As SalesOrderDetails;


CREATE VIEW SalesOrderDetails
AS
SELECT SalesOrderId, SalesOrderArray.[key]+1 as SalesOrderLine, SKUCode, SKUName,Price, Quantity
   FROM OPENROWSET('CosmosDB',
                'Account=adventureworks-mongodb;Database=AdventureWorks;Key=v2mtZ85W0AMCv1ZrY7jMUOWpfBTi1BrUz0Y3Rwmvj9SXSSIKDU7EQVu5kdEMcwAQfvJBnmHSMyxy50c3gD3v4g==',
                SalesOrder) 
                    WITH 
                    (   SalesOrderId varchar(max) '$._id.string', 
                        details varchar(max) '$.details.array'
                    )  As SalesOrders 
        CROSS APPLY OPENJSON(SalesOrders.details) AS SalesOrderArray
            CROSS APPLY OPENJSON(SalesOrderArray.[value]) 
            WITH
                (SKUCode varchar(max) '$.object.sku.string',
                SKUName varchar(max)    '$.object.name.string',
                Price decimal(10,4) '$.object.price.float64' ,
                Quantity int    '$.object.quantity.int32'
            ) As SalesOrderDetails;



SELECT TOP(10) * FROM SalesOrderDetails;


SELECT TOP(10) * FROM 
        SalesOrders
            INNER JOIN SalesOrderDetails
                ON SalesOrders.SalesOrderId = SalesOrderDetails.SalesOrderID;


CREATE VIEW SalesOrderStats
AS
SELECT
      o.Country, o.City,
      COUNT(DISTINCT o.CustomerId) Total_Customers,
      COUNT(DISTINCT d.SalesOrderId) Total_Orders,
      COUNT(d.SalesOrderId) Total_OrderLines,
      SUM(d.Quantity*d.Price) AS Total_Revenue,
      dense_rank() OVER (ORDER BY SUM(d.Quantity*d.Price) DESC) as Rank_Revenue,
      dense_rank() OVER (ORDER BY COUNT(DISTINCT d.SalesOrderId) DESC) as Rank_Orders,
      dense_rank() OVER (ORDER BY COUNT(d.SalesOrderId) DESC) as Rank_OrderLines,
      dense_rank() OVER (PARTITION BY o.Country ORDER BY SUM(d.Quantity*d.Price) DESC) as Rank_Revenue_Country
FROM SalesOrders o
 INNER JOIN SalesOrderDetails d
    ON o.SalesOrderId = d.SalesOrderId
WHERE Country IS NOT NULL OR City IS NOT NULL
GROUP BY o.Country, o.City
GO

SELECT * FROM SalesOrderStats
GO

