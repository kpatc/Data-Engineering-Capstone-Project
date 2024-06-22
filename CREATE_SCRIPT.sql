-- Create the table

---------------------------------------
CREATE TABLE public."DimDate"
(
    dateid integer NOT NULL,
    date date,
    Year integer,
    Quarter integer,
    QuarterName character(50),
    Month integer,
    Monthname character(50),
    Day integer,
    Weekday integer,
    WeekdayName character(50),
    CONSTRAINT "DimDate_pkey" PRIMARY KEY (dateid)
)

-------------------------------------------------------

CREATE TABLE public."DimCategory"
(
    categoryid integer NOT NULL,
    category character(50),
    CONSTRAINT "DimCategory_pkey" PRIMARY KEY (categoryid)
);

-------------------------------------------------------

CREATE TABLE public."DimCountry"
(
    countryid integer NOT NULL,
    country character(50),
    CONSTRAINT "DimCountry_pkey" PRIMARY KEY (countryid)
);

-----------------------------------------------------------

CREATE TABLE public."FactSales"
(
    Ordered integer NOT NULL,
    dateid integer,
    countryid integer,
    categoryid integer,
    amount integer,
    CONSTRAINT "FactSales_pkey" PRIMARY KEY (ordered)
);

-------------------------------------------------------------
\copy public."DimCategory" FROM '/home/project/DimCategory.csv' DELIMITER ',' CSV HEADER;

\copy public."FactSales" FROM '/home/project/FactSales.csv' DELIMITER ',' CSV HEADER;

------------------------------------------------------------------------------------------------------
--creating group query
SELECT 
    "DimCountry".country,
    "DimCategory".category,
    COALESCE(SUM("FactSales".amount), 0) AS totalsales
FROM 
    "DimCountry"
FULL OUTER JOIN "FactSales" ON "DimCountry".countryid = "FactSales".countryid
FULL OUTER JOIN "DimCategory" ON "FactSales".categoryid = "DimCategory".categoryid
GROUP BY 
    GROUPING SETS (("DimCountry".country), ("DimCategory".category), ("DimCountry".country, "DimCategory".category));
--------------------------------------------------------------------------------------------------------------------
----creating rollup query

SELECT 
    "DimDate".Year,
    "DimCountry".country,
    COALESCE(SUM("FactSales".amount), 0) AS totalsales
FROM 
    "FactSales"
LEFT JOIN "DimDate" ON "FactSales".dateid = "DimDate".dateid
LEFT JOIN "DimCountry" ON "FactSales".countryid = "DimCountry".countryid
GROUP BY 
    ROLLUP ("DimDate".Year, "DimCountry".country);

----------------------------------------------------------------------------------------------------------------
-----cubic query

SELECT 
    "DimDate".Year,
    "DimCountry".country,
    COALESCE(AVG("FactSales".amount), 0) AS averagesales
FROM 
    "FactSales"
LEFT JOIN "DimDate" ON "FactSales".dateid = "DimDate".dateid
LEFT JOIN "DimCountry" ON "FactSales".countryid = "DimCountry".countryid
GROUP BY 
    CUBE ("DimDate".Year, "DimCountry".country);

-------------------------------------------------------------------------------------------------------------------
-----create MQT

CREATE MATERIALIZED VIEW total_sales_per_country AS
SELECT
    "DimCountry".country,
    COALESCE(SUM("FactSales".amount), 0) AS total_sales
FROM
    "FactSales"
LEFT JOIN
    "DimCountry" ON "FactSales".countryid = "DimCountry".countryid
GROUP BY
    "DimCountry".country;

----------------------------------------------------------------------------------------------------------------------------------
---creating sales_data table
CREATE TABLE public.sales_data
(
    rowid integer NOT NULL,
    product_id integer,
    customer_id integer,
    price decimal DEFAULT 0.0 NOT NULL,
    quantity integer,
    timeestamp timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT "salesdata_pkey" PRIMARY KEY (rowid)
);

------------------------loading data in the table

\copy public.sales_data FROM '/home/project/sales-csv3mo8i5SHvta76u7DzUfhiw.csv' DELIMITER ',' CSV HEADER;



