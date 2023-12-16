-- Q1
select
    sup.SupplierID,
    sup.SupplierName,
    d.Quarter,
    d.Month,
    SUM(s.QuantityOrdered * s.ProductPrice) AS TotalSales
from
    Sales s
join Product p ON s.ProductID = p.ID
join Supplier sup ON p.SupplierID = sup.ID
join Date_D d ON s.DateID = d.DateID
group by
    sup.SupplierID, sup.SupplierName, d.Quarter, d.Month
order by
    sup.SupplierID, d.Quarter, d.Month;
    
    
-- Q2
select
    p.ProductID,
    p.ProductName,
    d.Month,
    SUM(s.QuantityOrdered * s.ProductPrice) AS TotalSales
from
    Sales s
join
    Product p ON s.ProductID = p.ID
join
    Supplier sup ON p.SupplierID = sup.ID
join
    Date_D d ON s.DateID = d.DateID
where
    sup.SupplierName = 'DJI'
    and d.Year = 2019
group by
    p.ProductID, p.ProductName, d.Month WITH ROLLUP
order by
    p.ProductID, d.Month;


-- Q3
select p.ProductID, p.ProductName, COUNT(*) AS SalesCount
from Product p
join Sales s ON p.ID = s.ProductID
join Date_D d ON s.DateID = d.DateID
where d.Weekend = true
group by p.ProductID, p.ProductName
order by SalesCount DESC
limit 5;

-- Q4
select
    p.ProductID,
    p.ProductName,
    SUM(case when d.Quarter = 1 then s.QuantityOrdered * s.ProductPrice else 0 end) as Q1_Sales,
    SUM(case when d.Quarter = 2 then s.QuantityOrdered * s.ProductPrice else 0 end) as Q2_Sales,
    SUM(case when d.Quarter = 3 then s.QuantityOrdered * s.ProductPrice else 0 end) as Q3_Sales,
    SUM(case when d.Quarter = 4 then s.QuantityOrdered * s.ProductPrice else 0 end) as Q4_Sales,
    SUM(s.QuantityOrdered * s.ProductPrice) AS Yearly_Sales
from
    Product p
join
    Sales s ON p.ID = s.ProductID
join
    Date_D d ON s.DateID = d.DateID
where
    d.Year = 2019
group by
    p.ProductID, p.ProductName
order by
    p.ProductID, p.ProductName; -- Complete the ORDER BY clause


-- Q5
select
    p.ProductID,
    p.ProductName,
    AVG(s.QuantityOrdered) AS AvgQuantity,
    ABS(AVG(s.QuantityOrdered) - s.QuantityOrdered) AS QuantityDeviation
from
    Sales s
join
    Product p ON s.ProductID = p.ID
group by
    p.ProductID, p.ProductName, s.QuantityOrdered
having
    QuantityDeviation > 2 * STDDEV(s.QuantityOrdered)
limit 0, 1000;

drop view if exists CUSTOMER_STORE_SALES_MV ;

-- Q6
create view STOREANALYSIS_VIEW AS
select
    s.StoreID,
    p.ProductID,
    SUM(s.QuantityOrdered) AS STORE_TOTAL
from
    Sales s
join
    Product p ON s.ProductID = p.ID
group by
    s.StoreID, p.ProductID;

-- Q7
select
    p.ProductID,
    p.ProductName,
    d.Month,
    SUM(s.QuantityOrdered * s.ProductPrice) AS TotalSales
from
    Sales s
join
    Product p ON s.ProductID = p.ID
join
    Store st ON s.StoreID = st.ID
join
    Date_D d ON s.DateID = d.DateID
where
    st.StoreName = 'Tech Haven'
group by
    p.ProductID, p.ProductName, d.Month
order by
    p.ProductID, d.Month;

-- Q8
create view SUPPLIER_PERFORMANCE_MV AS
select
    sup.SupplierID,
    sup.SupplierName,
    d.Month,
    SUM(s.QuantityOrdered) AS MonthlySales
from
    Sales s
join
    Product p ON s.ProductID = p.ID
join
    Supplier sup ON p.SupplierID = sup.ID
join
    Date_D d ON s.DateID = d.DateID
group by
    sup.SupplierID, sup.SupplierName, d.Month;

-- Q9

select
    c.CustomerID,
    c.CustomerName,
    COUNT(distinct s.ProductID) AS UniqueProducts,
    SUM(s.QuantityOrdered * s.ProductPrice) AS TotalSales
from
    Customer c
join
    Sales s ON c.ID = s.CustomerID
join
    Product p ON s.ProductID = p.ID
join
    Date_D d ON s.DateID = d.DateID
where
    d.Year = 2019
group by
    c.CustomerID, c.CustomerName
order by
    TotalSales DESC
limit 5;

-- Q 10
create  view CUSTOMER_STORE_SALES_MV AS
select
    d.Month,
    s.StoreID,
    c.CustomerID,
    c.CustomerName,
    COUNT(distinct p.ProductID) AS UniqueProducts,
    SUM(s.QuantityOrdered * s.ProductPrice) AS TotalSales
from
    Sales s
join
    Product p ON s.ProductID = p.ID
join
    Date_D d ON s.DateID = d.DateID
join
    Customer c ON s.CustomerID = c.ID
group by
    d.Month, s.StoreID, c.CustomerID, c.CustomerName;



