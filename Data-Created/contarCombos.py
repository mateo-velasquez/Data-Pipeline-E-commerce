import pandas as pd
import matplotlib.pyplot as plt

# Suponiendo que ya generaste los datos con createOrders
df_orders = pd.read_csv("csv/fact_table20250511124632.csv")  # Asegúrate de que la ruta sea correcta

# Contar la frecuencia de cada combo
combo_counts = df_orders.groupby(
    ['Product_ID', 'Sales', 'Quantity', 'Discount', 'Profit', 'Shipping_Cost']
).size().reset_index(name='Frecuencia')

# Ordenar por frecuencia descendente
combo_counts_sorted = combo_counts.sort_values(by='Frecuencia', ascending=False)

# Mostrar las 10 combinaciones más frecuentes
print(combo_counts_sorted.head(10))

# Opcional: crear un identificador para mostrar en el gráfico
combo_counts_sorted['Combo'] = combo_counts_sorted.apply(
    lambda row: f"ID:{int(row['Product_ID'])} S:{row['Sales']}", axis=1
)

# Graficar las 10 combinaciones más frecuentes
plt
