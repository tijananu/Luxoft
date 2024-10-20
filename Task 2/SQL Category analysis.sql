-- Temporary table for Order Details
SELECT 
    od.OrderID,
    cat.CategoryID,
    cat.CategoryName,
    pro.ProductID,
    pro.ProductName,
    (od.UnitPrice * od.Quantity * (1 - od.Discount)) AS TotalSales
INTO #OrderDetails
FROM dbo.Categories cat 
INNER JOIN dbo.Products pro ON cat.CategoryID = pro.CategoryID 
INNER JOIN dbo.[Order Details] od ON pro.ProductID = od.ProductID;

-- Temporary table for Orders
SELECT 
    o.OrderID,
    o.CustomerID,
    CAST(o.OrderDate AS DATE) AS OrderDate,
    CAST(o.ShippedDate AS DATE) AS ShippedDate,
    SUM(od.TotalSales) AS TotalSales
INTO #Orders
FROM dbo.Orders o 
INNER JOIN #OrderDetails od ON o.OrderID = od.OrderID
GROUP BY o.OrderID, o.CustomerID, o.OrderDate, o.ShippedDate;

-- Sales per product and category
WITH SalesPerProduct AS (
    SELECT 
        CategoryID,
        ProductID,
        SUM(TotalSales) AS Sales
    FROM #OrderDetails
    GROUP BY CategoryID, ProductID
),
-- Sales of Top 5 Products within each Category
Top5ProductsSales AS (
    SELECT 
        CategoryID, 
        SUM(Sales) AS Top5Sales
    FROM (
        SELECT 
			ProductID,
            CategoryID,
            Sales,
            ROW_NUMBER() OVER (PARTITION BY CategoryID ORDER BY Sales DESC) AS Rank
        FROM SalesPerProduct
    ) AS RankedProducts
    WHERE Rank <= 5
    GROUP BY CategoryID
),
-- Total Sales and Average Order Value per Category
SalesPerCategory AS (
    SELECT 
        od.CategoryID, 
		od.CategoryName,
        SUM(od.TotalSales) AS TotalSales, 
		AVG(od.TotalSales) AS AverageOrderValue
    FROM #OrderDetails od
    GROUP BY od.CategoryID, od.CategoryName
),
--Number of Orders per Customer
OrderFrequency AS (
    SELECT 
		od.CategoryID,
        o.CustomerID,
        COUNT(o.OrderID) AS OrderCount
    FROM #Orders o INNER JOIN #OrderDetails od ON o.OrderID = od.OrderID
    GROUP BY od.CategoryID, o.CustomerID
)

SELECT 
    spc.CategoryName AS Category, 
    spc.TotalSales, 
    spc.AverageOrderValue AS AverageOrderValue,
    AVG(DATEDIFF(DAY, o.OrderDate, o.ShippedDate)) AS AverageTimeToShip,
    ISNULL(tp.Top5Sales, 0) / NULLIF(spc.TotalSales, 0) AS Top5Ratio,
    AVG(odf.OrderCount) AS AverageOrderFrequency
FROM SalesPerCategory spc 
LEFT JOIN Top5ProductsSales tp ON spc.CategoryID = tp.CategoryID
LEFT JOIN OrderFrequency odf ON spc.CategoryID = odf.CategoryID
LEFT JOIN #Orders o ON o.OrderID IN (
    SELECT od.OrderID 
    FROM #OrderDetails od 
    WHERE od.CategoryID = spc.CategoryID
)
GROUP BY spc.CategoryName, spc.TotalSales, spc.AverageOrderValue, tp.Top5Sales
ORDER BY spc.TotalSales DESC;

-- Dropping temporary tables
DROP TABLE #OrderDetails;
DROP TABLE #Orders;
