CREATE TABLE [dbo].[CustomerSource] (
    [CustomerID] [int] NOT NULL,
    [Title] [nvarchar](8),
    [FirstName] [nvarchar](50),
    [MiddleName] [nvarchar](50),
    [LastName] [nvarchar](50),
    [Suffix] [nvarchar](10),
    [CompanyName] [nvarchar](128),
    [SalesPerson] [nvarchar](256),
    [EmailAddress] [nvarchar](50),
    [Phone] [nvarchar](25)
)

CREATE TABLE dbo.[DimCustomer](
    [CustomerID] [int] NOT NULL,
    [Title] [nvarchar](8) NULL,
    [FirstName] [nvarchar](50) NOT NULL,
    [MiddleName] [nvarchar](50) NULL,
    [LastName] [nvarchar](50) NOT NULL,
    [Suffix] [nvarchar](10) NULL,
    [CompanyName] [nvarchar](128) NULL,
    [SalesPerson] [nvarchar](256) NULL,
    [EmailAddress] [nvarchar](50) NULL,
    [Phone] [nvarchar](25) NULL,
    [InsertedDate] [datetime] NOT NULL,
    [ModifiedDate] [datetime] NOT NULL,
    [HashKey] [char](66)
)
