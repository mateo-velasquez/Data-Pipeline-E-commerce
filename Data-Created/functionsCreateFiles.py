import pandas as pd
import random
import obtenerInfo as obt

# DATOS CREAR CUSTOMER

# En base al archivo de obtenerInfo.py
primer_customer_id = obt.GetMaxId('../Data-Processed/dim_costumer.csv', 'Customer_Id')+1
print(primer_customer_id)


# DATOS CREAR Orders

# En base al archivo de obtenerInfo.py
primer_order_id = obt.GetMaxId('../Data-Processed/fact_table.csv', 'Order_ID')+1
print(primer_order_id)

# Ahora generamos nuevos perfiles:

def createCustomers(cantidadPerfiles):
    genders = ["Female", "Male"]

    # Mayor probabilidad de "Member"
    tipeLogins = ["Guest"] + ["Member"] * 9

    data = []
    for i in range(cantidadPerfiles):
        customer_id = primer_customer_id + i
        login_type = random.choice(tipeLogins)
        gender = random.choice(genders)
        data.append([customer_id, login_type, gender])

    # Crear DataFrame
    df = pd.DataFrame(data, columns=["Customer_Id", "Customer_Login_type", "Gender"])
    return df

def createOrders(cantidadOrders, anio, cantidadPerfiles):
    # Rango de perfiles
    rangoPerfiles = range(primer_customer_id + 1, primer_customer_id + cantidadPerfiles)

    # Otros datos:
    device_types = ["Web"] * 20 + ["Mobile"]
    order_priorities = ["Low"] + ["Medium"] * 10 + ["High", "Critical"] * 5
    payment_methods = ["credit_card"] * 15 + ["e_wallet"] * 3 + ["money_order"] + ["debit_card"] * 5

    # valores de Sales y product_id:
    # Leer archivo existente
    df_fact = pd.read_csv('../Data-Processed/fact_table.csv')

    # Extraer combinaciones únicas de Product_ID y Sales
    product_sales_combos = df_fact[['Product_ID', 'Sales','Quantity', 'Discount', 'Profit', 'Shipping_Cost']].drop_duplicates().values.tolist()

    data = []

    for i in range(cantidadOrders):
        order_date = pd.to_datetime(f"{anio}-{random.randint(1, 12)}-{random.randint(1, 28)}")
        order_time = f"{random.randint(0, 23):02}:{random.randint(0, 59):02}:{random.randint(0, 59):02}"

        # Cargamos los combos de Sales y product_id:
        # Elegimos una combinación aleatoria válida de producto y precio
        combo = random.choice(product_sales_combos)
        product_id = int(combo[0])  # fuerza a entero
        sales = combo[1]
        quantity = combo[2]
        discount = combo[3]
        profit = combo[4]
        shipping_cost = combo[5]

        aging = round(random.uniform(1, 10), 1)
        customer_id = random.choice(rangoPerfiles)
        device = random.choice(device_types)

        priority = random.choice(order_priorities)
        payment = random.choice(payment_methods)

        order_id = primer_order_id + i

        data.append([
            order_date.date(), order_time, aging, customer_id, device,
            sales, float(quantity), discount, profit, shipping_cost,
            priority, payment, order_id, product_id
        ])

    # Crear DataFrame
    df = pd.DataFrame(data, columns=[
        "Order_Date", "Time", "Aging", "Customer_Id", "Device_Type",
        "Sales", "Quantity", "Discount", "Profit", "Shipping_Cost",
        "Order_Priority", "Payment_method", "Order_ID", "Product_ID"
    ])
    return df


