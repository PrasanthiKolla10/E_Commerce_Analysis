import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt 
import seaborn as sns 
import mysql.connector 

# connects to MySQL server
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Prasanthi@10',
    database='E_Commerce'
)
cursor = conn.cursor()       #activates the cursor

#List all unique cities where customers are located 
query = "SELECT DISTINCT city FROM customers"
cursor.execute(query)          #executes the above query 
data = cursor.fetchall()       #fetches from the dataset
df = pd.DataFrame(data) 
df 


#Count the number of orders placed in 2017 
query =   "SELECT 
            COUNT(order_ID)
          FROM
              orders
          WHERE
              (order_purchase_timestamp) = 2017"
cursor.execute(query)        
data = cursor.fetchall()     
"total orders placed in 2017 are", data[0][0]


#Find total sales per category 
query = "SELECT 
          UPPER(products.product_category),
          ROUND(SUM(payments.payment_value), 2)
        FROM
            products
                JOIN
            order_Items ON products.product_ID = order_items.product_id
                JOIN
            payments ON payments.order_id = order_items.order_ID
            GROUP BY product_category;"
cursor.execute(query)
data = cursor.fetchall()
df = pd.DataFrame(data, columns = ["Category", "Sales"])



# Calculate the percentage of orders that were paid in installment
query = "SELECT 
            SUM(CASE
                WHEN payment_installments >= 1 THEN 1
                ELSE 0
            END) / COUNT(*) * 100
        FROM
            payments;"
cursor.execute(query)
data = cursor.fetchall()


# Count number of customers in each state
query = "SELECT 
              customer_state, COUNT(customer_ID)
          FROM
              customers
          GROUP BY customer_state;"
cursor.execute(query) 
data = cursor.fetchall()

df = pd.DataFrame(data, columns = ["state", "customer_count"]) 
df = df.sort_values(by = "customer_count", ascending = False)

plt.figure(figsize = (9, 4))   #adjusts the size of the chart
plt.bar(df["state"], df["customer_count"])
plt.xticks(rotation = 90)   #to rotate x-axis 
plt.show()


# Calcuate no.of orders per month in 2018
query = "SELECT 
              MONTH(order_purchase_timestamp) months,
              COUNT(order_ID) order_count
          FROM
              orders
          WHERE
              YEAR(order_purchase_timestamp) = 2018
          GROUP BY months;"
cursor.execute(query) 
data = cursor.fetchall()

df = pd.DataFrame(data, columns = ["Month", "Order_Count"])
df

o = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]

ax = sns.barplot(x = df["Month"], y = df["Order_Count"], data = df , order = o, hue = df["Month"] , palette = "viridis")
                                                                                # simply we can give a color = "red"
plt.xticks(rotation = 45)
ax.bar_label(ax.containers[0])
plt.xlabel("states")
plt.ylabel("customers_count")
plt.title("count of customers by states")
plt.show()


# Find average no.of products per order grouped by customer city
query = "with count_per_order AS 
            (SELECT orders.order_id, orders.customer_id, count(order_items.order_id) as oc 
            FROM orders JOIN order_items ON orders.order_id = order_items.order_id  
            GROUP BY orders.order_id, orders.customer_id)
            SELECT customers.customer_city, avg(count_per_order.oc) as average_count  
            FROM customers JOIN count_per_order ON customers.customer_id = count_per_order.customer_id 
            GROUP BY customers.customer_city;"
cursor.execute(query) 
data = cursor.fetchall()
df = pd.DataFrame(data, columns = ["customer city", "average products/orders"])
df.head(10)


# Calculate the percentage of total revenue contributed by each product category
query = " SELECT 
                  UPPER(products.product_category) category,
                  ROUND(SUM(payments.payment_value) / (SELECT 
                                  SUM(payment_value)
                              FROM
                                  payments) * 100,
                          2) sales_percentage
              FROM
                  products
                      JOIN
                  order_items ON products.product_id = order_items.product_id
                      JOIN
                  payments ON payments.order_id = order_items.order_id
              GROUP BY category
              ORDER BY sales_percentage DESC;"
cursor.execute(query)
data = cursor.fetchall()
df = pd.DataFrame(data, columns = ["Category", "% distribution"])
df.head()

#To show the percentage distribution 
plt.pie(df["% distribution"], labels = df["Category"])
plt.show()


# Identify the "correlation" between product price & the number of times a product has been purchased
query = "SELECT 
                  products.product_category,
                  COUNT(order_items.product_id),
                  ROUND(AVG(order_items.price), 2)
              FROM
                  products
                      JOIN
                  order_items ON products.product_id = order_items.product_id
              GROUP BY products.product_category;" 

cursor.execute(query)
data = cursor.fetchall()
df = pd.DataFrame(data, columns = ["Category", "Order_count", "Price"])
df

#numpy 
arr1 = df["Order_count"]
arr2 = df["Price"]

a = np.corrcoef([arr1,arr2])   
print("the correlation between price & number of times a product has been purchased", a[0][1])


# Calculate the total revenue generated by each seller & rank them by revenue
query = "SELECT *, DENSE_RANK() OVER(ORDER BY revenue DESC) AS rn 
            FROM (SELECT order_items.seller_id, sum(payment_value) revenue 
            FROM order_items JOIN payments 
            ON order_items.order_id = payments.order_id 
            GROUP BY order_items.seller_id) AS a;"
cursor.execute(query)
data = cursor.fetchall()
df = pd.DataFrame(data, columns = ["seller_id", "revenue", "rank"])
df = df.head()

sns.barplot(x = "seller_id", y = "revenue", data = df) 
plt.xticks(rotation = 90)
plt.show()


#Calculate the moving average of order values of each customer on their order_history
query = "SELECT customer_id, order_purchase_timestamp, payment, aVG(payment) 
              OVER (PARTITION BY customer_id 
              ORDER BY order_purchase_timestamp ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) 
              AS mov_avg FROM  
              (SELECT orders.customer_id, orders.order_purchase_timestamp, payments.payment_value 
              AS payment FROM payments JOIN orders 
              ON payments.order_id = orders.order_id) AS a;" 
cursor.execute(query) 
data = cursor.fetchall()
df = pd.DataFrame(data, columns = ["order_id", "time_stamp","Price", "Moving_average"])
df


# Calculate the cumulative sales per month for each year
query = "SELECT years, months, payment, sum(payment) 
            OVER(ORDER BY years, months) cumulative_sales 
            FROM (SELECT YEAR(orders.order_purchase_timestamp) AS years,  
            MONTH(orders.order_purchase_timestamp) AS months,  
            ROUND(SUM(payments.payment_value),2) AS payment 
            FROM orders JOIN payments ON  orders.order_id = payments.order_id 
            GROUP BY years, months 
            ORDER BY years, months) AS a;" 
cursor.execute(query)
data  = cursor.fetchall()
df = pd.DataFrame(data, columns = ["Year", "Month", "sales", "Price"])


# Calculate the year-over-year growth rate of total sales
query = """with a as (select year(orders.order_purchase_timestamp) as years, round(sum(payments.payment_value),2) 
            as payment from orders join payments on  
            orders.order_id = payments.order_id group by years 
            order by years)
    select years, payment, lag(payment, 1) over(order by years) from a"""
cursor.execute(query)
data = cursor.fetchall()
df = pd.DataFrame(data, columns = ["Years", "Sales", "Previous_Year"])
df
