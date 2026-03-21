import pandas as pd
import os

ruta = "Datos TFG/Desarrollo"

# Archivos desarrollo csv
archivos_variables = {
    "PIB per cápita.csv": "gdp_per_capita",
    "Tasa incidencia pobreza.csv": "tasa_pobreza",
    "Densidad de población.csv": "densidad_poblacion",
    "Inflación, deflactor del PIB (%) anual.csv": "deflactor_inflacion_gdp",
    "Tasa de natalidad bruta.csv": "tasa_natalidad",
    "Crecimiento de la población rural.csv": "crecimiento_poblacion_rural",
    "Población rural.csv": "poblacion_rural",
    "Tasa de mortalidad infantil.csv": "tasa_mortalidad_infantil",
    "hdi_limpio.csv": "hdi"
}

# Leer archivos World Bank
def leer_desarrollo_wb(ruta_archivo, nombre_variable):
    df = None

    # Probar distintos separadores y codificaciones
    configuraciones = [
        {"sep": ";", "encoding": "latin1"},
        {"sep": ",", "encoding": "utf-8-sig"},
        {"sep": ",", "encoding": "latin1"},
        {"sep": ";", "encoding": "utf-8-sig"}
    ]

    for config in configuraciones:
        try:
            temp = pd.read_csv(
                ruta_archivo,
                sep=config["sep"],
                engine="python",
                encoding=config["encoding"],
                skiprows=4
            )
            temp.columns = [str(col).strip().replace("\ufeff", "") for col in temp.columns]

            if "Country Code" in temp.columns:
                df = temp.copy()
                break
        except Exception:
            continue

    if df is None:
        raise ValueError(f"No se pudo leer correctamente el archivo {ruta_archivo}")

    # Renombrar columna country_code
    df = df.rename(columns={"Country Code": "country_code"})

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

# Leer HDI limpio
def leer_hdi_limpio(ruta_archivo, nombre_variable):
    df = pd.read_csv(ruta_archivo)

    df.columns = [str(col).strip().replace("\ufeff", "") for col in df.columns]

    # Renombrar la columna hdi al nombre_variable por consistencia
    df = df.rename(columns={"hdi": nombre_variable})

    # Nos quedamos solo con las columnas necesarias
    df = df[["country_code", "year", nombre_variable]].copy()

    # Convertir tipos
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df[nombre_variable] = pd.to_numeric(df[nombre_variable], errors="coerce")

    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    return df

# Cargar datasets
lista_dfs = []

for archivo, nombre_variable in archivos_variables.items():
    ruta_archivo = os.path.join(ruta, archivo)

    if not os.path.exists(ruta_archivo):
        print(f"No encontrado: {archivo}")
        continue

    try:
        if archivo == "hdi_limpio.csv":
            df = leer_hdi_limpio(ruta_archivo, nombre_variable)
        else:
            df = leer_desarrollo_wb(ruta_archivo, nombre_variable)

        lista_dfs.append(df)
        print(f"OK: {archivo}")

    except Exception as e:
        print(f"Error en {archivo}: {e}")

if len(lista_dfs) == 0:
    raise ValueError("No se ha podido cargar ningún dataset.")

# Unir todos los datasets por country_code y year
df_desarrollo = lista_dfs[0]

for df in lista_dfs[1:]:
    df_desarrollo = df_desarrollo.merge(df, on=["country_code", "year"], how="outer")

# Revisar resultado
print("\nPrimeras filas:")
print(df_desarrollo.head())

print("\nInformación general:")
print(df_desarrollo.info())

# Guardar dataset final
df_desarrollo.to_csv("dataset_desarrollo_unificado.csv", index=False)
print("\nArchivo guardado como: dataset_desarrollo_unificado.csv")