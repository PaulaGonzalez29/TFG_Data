import pandas as pd
import os

ruta = "Datos TFG/Electrificacion"

# Archivos electrificación csv
archivos_variables = {
    "Acceso a electricidad.csv": "electricity_access_total",
    "Access to electricity, rural.csv": "electricity_access_rural",
    "Access to electricity, urban.csv": "electricity_access_urban",
    "Consumo de energía eléctrica.csv": "electricity_consumption",
    "PIB por unidad de uso de energía.csv": "gdp_per_unit_energy_use",
    "Inversión en energía con particp privada.csv": "private_investment_energy",
    "Consumer Affordability of Electricity.csv": "consumer_affordability_electricity"
}

# Función para leer archivos de electrificación con estructura World Bank
def leer_electrificacion(ruta_archivo, nombre_variable):
    df = pd.read_csv(
        ruta_archivo,
        sep=";",
        engine="python",
        encoding="latin1",
        skiprows=4
    )
    
    # Normalizar nombres de columnas
    df.columns = [str(col).strip().replace("\ufeff", "") for col in df.columns]
    df = df.rename(columns={"Country Code": "country_code"})
    
    # Comprobar que existe country_code
    if "country_code" not in df.columns:
        raise ValueError(f"No se encontró la columna 'Country Code' en {ruta_archivo}")

    # Eliminar columnas que no necesitamos
    df = df.drop(columns=["Country Name", "Indicator Name", "Indicator Code"], errors="ignore")

    # Pasar de ancho a largo
    df = df.melt(
        id_vars=["country_code"],
        var_name="year",
        value_name=nombre_variable
    )

    # Convertir year a numérico
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    return df

# Función para leer Consumer Affordability ya corregido manualmente
def leer_affordability(ruta_archivo, nombre_variable):
    df = pd.read_csv(ruta_archivo)

    # Renombrar columna del valor
    df = df.rename(columns={"OBS_VALUE": nombre_variable})

    # Quedarnos solo con las columnas necesarias
    df = df[["country_code", "year", nombre_variable]].copy()

    # Convertir year a numérico
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    return df

lista_dfs = []

for archivo, nombre_variable in archivos_variables.items():
    ruta_archivo = os.path.join(ruta, archivo)

    if not os.path.exists(ruta_archivo):
        print(f"No encontrado: {archivo}")
        continue

    try:
        if archivo == "Consumer Affordability of Electricity.csv":
            df = leer_affordability(ruta_archivo, nombre_variable)
        else:
            df = leer_electrificacion(ruta_archivo, nombre_variable)

        lista_dfs.append(df)
        print(f"OK: {archivo}")

    except Exception as e:
        print(f"Error en {archivo}: {e}")

if len(lista_dfs) == 0:
    raise ValueError("No se ha podido cargar ningún dataset.")

# Unir todos los datasets por country_code y year
df_electrificacion = lista_dfs[0]

for df in lista_dfs[1:]:
    df_electrificacion = df_electrificacion.merge(df, on=["country_code", "year"], how="outer")

# Revisar resultado
print("\nPrimeras filas:")
print(df_electrificacion.head())

print("\nInformación general:")
print(df_electrificacion.info())

# Guardar dataset final
df_electrificacion.to_csv("dataset_electrificacion_unificado.csv", index=False)
print("\nArchivo guardado como: dataset_electrificacion_unificado.csv")