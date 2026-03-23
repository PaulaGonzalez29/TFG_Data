import pandas as pd
import os

ruta = "Datos TFG/Desarrollo"

# Archivos a integrar
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

# --------------------------------------------------
# LECTURA DE ARCHIVOS WORLD BANK
# --------------------------------------------------
def leer_desarrollo_wb(ruta_archivo, nombre_variable):
    df = None

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

    # Renombrar columna clave
    df = df.rename(columns={"Country Code": "country_code"})

    # Eliminar columnas innecesarias
    df = df.drop(columns=["Country Name", "Indicator Name", "Indicator Code"], errors="ignore")

    # Pasar de ancho a largo
    df = df.melt(
        id_vars=["country_code"],
        var_name="year",
        value_name=nombre_variable
    )

    # Limpiar year
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    # Limpiar valor
    df[nombre_variable] = pd.to_numeric(df[nombre_variable], errors="coerce")

    return df


# --------------------------------------------------
# LECTURA DE HDI LIMPIO
# --------------------------------------------------
def leer_hdi_limpio(ruta_archivo, nombre_variable):
    df = pd.read_csv(ruta_archivo, encoding="utf-8-sig")

    df.columns = [str(col).strip().replace("\ufeff", "") for col in df.columns]

    columnas_esperadas = ["country_code", "year", "hdi"]
    for col in columnas_esperadas:
        if col not in df.columns:
            raise ValueError(f"En {ruta_archivo} falta la columna '{col}'")

    # Renombrar hdi al nombre de variable definido en el diccionario
    df = df.rename(columns={"hdi": nombre_variable})

    # Quedarnos solo con lo necesario
    df = df[["country_code", "year", nombre_variable]].copy()

    # Limpiar tipos
    df["country_code"] = df["country_code"].astype(str).str.strip().str.upper()
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df[nombre_variable] = pd.to_numeric(df[nombre_variable], errors="coerce")

    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    return df


# --------------------------------------------------
# CARGA DE DATASETS
# --------------------------------------------------
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
        print(f"OK: {archivo} -> {nombre_variable}")

    except Exception as e:
        print(f"Error en {archivo}: {e}")

if len(lista_dfs) == 0:
    raise ValueError("No se ha podido cargar ningún dataset.")


# --------------------------------------------------
# UNIÓN DE TODOS LOS DATASETS
# --------------------------------------------------
df_desarrollo = lista_dfs[0]

for df in lista_dfs[1:]:
    df_desarrollo = df_desarrollo.merge(df, on=["country_code", "year"], how="outer")


# --------------------------------------------------
# LIMPIEZA FINAL
# --------------------------------------------------
df_desarrollo["country_code"] = df_desarrollo["country_code"].astype(str).str.strip().str.upper()
df_desarrollo["year"] = pd.to_numeric(df_desarrollo["year"], errors="coerce")
df_desarrollo = df_desarrollo.dropna(subset=["country_code", "year"])
df_desarrollo["year"] = df_desarrollo["year"].astype(int)

# Ordenar para revisar mejor
df_desarrollo = df_desarrollo.sort_values(by=["country_code", "year"]).reset_index(drop=True)


# --------------------------------------------------
# REVISIÓN
# --------------------------------------------------
print("\nPrimeras filas:")
print(df_desarrollo.head())

print("\nColumnas del dataset final:")
print(df_desarrollo.columns.tolist())

print("\nInformación general:")
print(df_desarrollo.info())


# --------------------------------------------------
# GUARDAR RESULTADO
# --------------------------------------------------
salida = "dataset_desarrollo_unificado.csv"
df_desarrollo.to_csv(salida, index=False, encoding="utf-8-sig")
print(f"\nArchivo guardado como: {salida}")