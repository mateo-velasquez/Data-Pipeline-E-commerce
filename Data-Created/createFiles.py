import pandas as pd
import time as Time
import functionsCreateFiles as fcf

def crearFileCustomers(cantidaPerfiles):
    # Generar perfiles
    df_customers = fcf.createCustomers(cantidaPerfiles)

    # Guardar el CSV con timestamp
    timestamp = Time.strftime("%Y%m%d%H%M%S")
    nombre_archivo = f"csv/dim_customer{timestamp}.csv"
    df_customers.to_csv(nombre_archivo, index=False)

    return f"Archivo generado: {nombre_archivo}"

def crearFileOrders(cantidadOrders, anio, cantidaPerfiles):
    # Generar orders
    df_customers = fcf.createOrders(cantidadOrders, anio, cantidaPerfiles)

    # Guardar el CSV con timestamp
    timestamp = Time.strftime("%Y%m%d%H%M%S")
    nombre_archivo = f"csv/fact_table{timestamp}.csv"
    df_customers.to_csv(nombre_archivo, index=False)

    return f"Archivo generado: {nombre_archivo}"

#print(crearFileCustomers(70000))
#print(crearFileOrders(20000,2019,20000))
print(crearFileOrders(30500,2020,40000))
print(crearFileOrders(31500,2021,51000))
print(crearFileOrders(39700,2022,51000))
print(crearFileOrders(38100,2023,62000))
print(crearFileOrders(33400,2024,70000))
