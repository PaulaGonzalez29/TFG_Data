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

    df = df.rename(columns={"Country Code": "country_code"})
    df["country_code"] = df["country_code"].astype(str).str.strip().str.upper()

    df = df.drop(columns=["Country Name", "Indicator Name", "Indicator Code"], errors="ignore")

    df = df.melt(
        id_vars=["country_code"],
        var_name="year",
        value_name=nombre_variable
    )

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    df[nombre_variable] = pd.to_numeric(df[nombre_variable], errors="coerce")

    return df


# --------------------------------------------------
# LECTURA DE HDI LIMPIO
# --------------------------------------------------
def leer_hdi_limpio(ruta_archivo, nombre_variable):
    configuraciones = [
        {"sep": ",", "encoding": "utf-8-sig"},
        {"sep": ";", "encoding": "utf-8-sig"},
        {"sep": ",", "encoding": "latin1"},
        {"sep": ";", "encoding": "latin1"}
    ]

    df = None

    for config in configuraciones:
        try:
            temp = pd.read_csv(
                ruta_archivo,
                sep=config["sep"],
                engine="python",
                encoding=config["encoding"]
            )
            temp.columns = [str(col).strip().replace("\ufeff", "") for col in temp.columns]
            if len(temp.columns) >= 3:
                df = temp.copy()
                break
        except Exception:
            continue

    if df is None:
        raise ValueError(f"No se pudo leer correctamente el archivo {ruta_archivo}")

    # Renombrar posibles columnas del código país
    if "country_code" not in df.columns:
        if "iso3" in df.columns:
            df = df.rename(columns={"iso3": "country_code"})
        elif "iso_code" in df.columns:
            df = df.rename(columns={"iso_code": "country_code"})
        elif "Country Code" in df.columns:
            df = df.rename(columns={"Country Code": "country_code"})
        elif "geoUnit" in df.columns:
            df = df.rename(columns={"geoUnit": "country_code"})

    # Renombrar posibles columnas del valor HDI
    if "hdi" not in df.columns:
        if "HDI" in df.columns:
            df = df.rename(columns={"HDI": "hdi"})
        elif "value" in df.columns:
            df = df.rename(columns={"value": "hdi"})
        elif "OBS_VALUE" in df.columns:
            df = df.rename(columns={"OBS_VALUE": "hdi"})

    columnas_esperadas = ["country_code", "year", "hdi"]
    for col in columnas_esperadas:
        if col not in df.columns:
            raise ValueError(
                f"En {ruta_archivo} falta la columna '{col}'. "
                f"Columnas detectadas: {df.columns.tolist()}"
            )

    df = df.rename(columns={"hdi": nombre_variable})
    df = df[["country_code", "year", nombre_variable]].copy()

    df["country_code"] = df["country_code"].astype(str).str.strip().str.upper()
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df[nombre_variable] = pd.to_numeric(df[nombre_variable], errors="coerce")

    df = df.dropna(subset=["country_code", "year"])
    df = df[df["country_code"].str.match(r"^[A-Z]{3}$", na=False)]
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
        print(f"OK: {archivo} -> {nombre_variable} | filas: {len(df)}")

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
df_desarrollo = df_desarrollo[df_desarrollo["country_code"].str.match(r"^[A-Z]{3}$", na=False)]
df_desarrollo["year"] = df_desarrollo["year"].astype(int)

df_desarrollo = df_desarrollo.sort_values(by=["country_code", "year"]).reset_index(drop=True)


# --------------------------------------------------
# REVISIÓN
# --------------------------------------------------
print("\nPrimeras filas:")
print(df_desarrollo.head())

print("\nColumnas del dataset final:")
print(df_desarrollo.columns.tolist())

if "hdi" in df_desarrollo.columns:
    print("\nValores no nulos en hdi:", df_desarrollo["hdi"].notna().sum())
else:
    print("\nLa columna hdi no está en el dataset final.")

print("\nInformación general:")
print(df_desarrollo.info())


# --------------------------------------------------
# GUARDAR RESULTADO
# --------------------------------------------------
salida = "dataset_desarrollo_unificado.csv"
df_desarrollo.to_csv(salida, index=False, encoding="utf-8-sig")
print(f"\nArchivo guardado como: {salida}")