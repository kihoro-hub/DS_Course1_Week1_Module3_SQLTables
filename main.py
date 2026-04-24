import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

print(pd.read_sql("SELECT * FROM sqlite_master", conn))

# ── Step 1: Boston Employees ───────────────────────────────────────────────────
df_boston = pd.read_sql("""
    SELECT e.firstName, e.lastName, e.jobTitle
    FROM employees e
    JOIN offices o ON e.officeCode = o.officeCode
    WHERE o.city = 'Boston'
""", conn)
print("\nStep 1 - Boston Employees:")
print(df_boston)

# ── Step 2: Offices with Zero Employees ───────────────────────────────────────
df_zero_emp = pd.read_sql("""
    SELECT o.officeCode, o.city
    FROM offices o
    LEFT JOIN employees e ON o.officeCode = e.officeCode
    GROUP BY o.officeCode
    HAVING COUNT(e.employeeNumber) = 0
""", conn)
print("\nStep 2 - Offices with Zero Employees:")
print(df_zero_emp)

# ── Step 3: All Employees with Office City/State ──────────────────────────────
df_employee = pd.read_sql("""
    SELECT e.firstName, e.lastName, o.city, o.state
    FROM employees e
    LEFT JOIN offices o ON e.officeCode = o.officeCode
    ORDER BY e.firstName, e.lastName
""", conn)
print("\nStep 3 - All Employees with Office Info:")
print(df_employee)

# ── Step 4: Customers with No Orders ──────────────────────────────────────────
df_contacts = pd.read_sql("""
    SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
    FROM customers c
    LEFT JOIN orders o ON c.customerNumber = o.customerNumber
    WHERE o.orderNumber IS NULL
    ORDER BY c.contactLastName
""", conn)
print("\nStep 4 - Customers with No Orders:")
print(df_contacts)

# ── Step 5: Customer Payments Sorted by Amount ────────────────────────────────
df_payment = pd.read_sql("""
    SELECT c.contactFirstName, c.contactLastName, p.amount, p.paymentDate
    FROM customers c
    JOIN payments p ON c.customerNumber = p.customerNumber
    ORDER BY CAST(p.amount AS REAL) DESC
""", conn)
print("\nStep 5 - Customer Payments:")
print(df_payment)

# ── Step 6: Employees with Avg Customer Credit Limit > 90k ────────────────────
df_credit = pd.read_sql("""
    SELECT e.employeeNumber, e.firstName, e.lastName, COUNT(c.customerNumber) AS numCustomers
    FROM employees e
    JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY e.employeeNumber
    HAVING AVG(c.creditLimit) > 90000
    ORDER BY numCustomers DESC
""", conn)
print("\nStep 6 - Employees with High Avg Credit Limit Customers:")
print(df_credit)

# ── Step 7: Top Selling Products ──────────────────────────────────────────────
df_product_sold = pd.read_sql("""
    SELECT p.productName,
           COUNT(od.orderNumber) AS numorders,
           SUM(od.quantityOrdered) AS totalunits
    FROM products p
    JOIN orderdetails od ON p.productCode = od.productCode
    GROUP BY p.productCode
    ORDER BY totalunits DESC
""", conn)
print("\nStep 7 - Top Selling Products:")
print(df_product_sold)

# ── Step 8: Unique Customers per Product ──────────────────────────────────────
df_total_customers = pd.read_sql("""
    SELECT p.productName, p.productCode,
           COUNT(DISTINCT o.customerNumber) AS numpurchasers
    FROM products p
    JOIN orderdetails od ON p.productCode = od.productCode
    JOIN orders o ON od.orderNumber = o.orderNumber
    GROUP BY p.productCode
    ORDER BY numpurchasers DESC
""", conn)
print("\nStep 8 - Unique Customers per Product:")
print(df_total_customers)

# ── Step 9: Customers per Office ──────────────────────────────────────────────
df_customers = pd.read_sql("""
    SELECT o.officeCode, o.city,
           COUNT(c.customerNumber) AS n_customers
    FROM offices o
    JOIN employees e ON o.officeCode = e.officeCode
    JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY o.officeCode
""", conn)
print("\nStep 9 - Customers per Office:")
print(df_customers)

# ── Step 10: Employees Who Sold Under-20-Customer Products ────────────────────
df_under_20 = pd.read_sql("""
    WITH under_20_products AS (
        SELECT od.productCode
        FROM orderdetails od
        JOIN orders o ON od.orderNumber = o.orderNumber
        GROUP BY od.productCode
        HAVING COUNT(DISTINCT o.customerNumber) < 20
    )
    SELECT DISTINCT e.employeeNumber, e.firstName, e.lastName, o.city, o.officeCode
    FROM employees e
    JOIN offices o ON e.officeCode = o.officeCode
    JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    JOIN orders ord ON c.customerNumber = ord.customerNumber
    JOIN orderdetails od ON ord.orderNumber = od.orderNumber
    WHERE od.productCode IN (SELECT productCode FROM under_20_products)
""", conn)
print("\nStep 10 - Employees Who Sold Under-20-Customer Products:")
print(df_under_20)

# ── Close the connection ───────────────────────────────────────────────────────
conn.close()
