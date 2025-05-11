import pandas as pd

def GetMaxId(archivo_csv, type_id):
    # Cargar el CSV en un DataFrame
    df = pd.read_csv(archivo_csv)

    # Obtener el mayor Customer_Id
    mayor_id = df[type_id].max()
    #print(f"El mayor Customer_Id es: {mayor_id}") # 99999

    return mayor_id