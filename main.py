import sqlite3
import pandas as pd

conn = sqlite3.connect("data.sqlite")

df_boston = pd.read_sql("""
SELECT firstName, lastName
FROM employees
JOIN offices
ON employees.officeCode = offices.officeCode
WHERE offices.city = 'Boston'
ORDER BY firstName, lastName
""", conn)

df_zero_emp = pd.read_sql("""
SELECT o.officeCode, o.city
FROM offices o
LEFT JOIN employees e
ON o.officeCode = e.officeCode
WHERE e.employeeNumber IS NULL
""", conn)

df_employee = pd.read_sql("""
SELECT firstName, lastName, city, state
FROM employees
LEFT JOIN offices
ON employees.officeCode = offices.officeCode
ORDER BY firstName, lastName
""", conn)

df_contacts = pd.read_sql("""
SELECT contactFirstName,
       contactLastName,
       phone,
       salesRepEmployeeNumber
FROM customers
LEFT JOIN orders
ON customers.customerNumber = orders.customerNumber
WHERE orders.orderNumber IS NULL
ORDER BY contactLastName
""", conn)

df_payment = pd.read_sql("""
SELECT contactFirstName,
       contactLastName,
       paymentDate,
       amount
FROM customers
JOIN payments
ON customers.customerNumber = payments.customerNumber
ORDER BY CAST(amount AS REAL) DESC
""", conn)

df_credit = pd.read_sql("""
SELECT e.employeeNumber,
       e.firstName,
       e.lastName,
       COUNT(c.customerNumber) AS numcustomers
FROM employees e
JOIN customers c
ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY e.employeeNumber
HAVING AVG(c.creditLimit) > 90000
ORDER BY numcustomers DESC
LIMIT 4
""", conn)

df_product_sold = pd.read_sql("""
SELECT p.productName,
       COUNT(d.orderNumber) AS numorders,
       SUM(d.quantityOrdered) AS totalunits
FROM products p
JOIN orderdetails d
ON p.productCode = d.productCode
GROUP BY p.productCode
ORDER BY totalunits DESC
""", conn)

df_total_customers = pd.read_sql("""
SELECT p.productName,
       p.productCode,
       COUNT(DISTINCT o.customerNumber) AS numpurchasers
FROM products p
JOIN orderdetails d
ON p.productCode = d.productCode
JOIN orders o
ON d.orderNumber = o.orderNumber
GROUP BY p.productCode
ORDER BY numpurchasers DESC
""", conn)

df_customers = pd.read_sql("""
SELECT o.officeCode,
       o.city,
       COUNT(c.customerNumber) AS n_customers
FROM offices o
LEFT JOIN employees e
ON o.officeCode = e.officeCode
LEFT JOIN customers c
ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY o.officeCode, o.city
ORDER BY n_customers DESC
""", conn)

df_under_20 = pd.read_sql("""
SELECT DISTINCT e.employeeNumber,
       e.firstName,
       e.lastName,
       o.city,
       o.officeCode
FROM employees e
JOIN offices o
ON e.officeCode = o.officeCode
JOIN customers c
ON e.employeeNumber = c.salesRepEmployeeNumber
JOIN orders od
ON c.customerNumber = od.customerNumber
JOIN orderdetails d
ON od.orderNumber = d.orderNumber
WHERE d.productCode IN (
    SELECT d2.productCode
    FROM orderdetails d2
    JOIN orders o2
    ON d2.orderNumber = o2.orderNumber
    GROUP BY d2.productCode
    HAVING COUNT(DISTINCT o2.customerNumber) < 20
)
ORDER BY e.employeeNumber
""", conn)