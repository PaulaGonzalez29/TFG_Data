import pandas as pd
import os

ruta = "Datos TFG/Electrificacion"

archivos_variables = {
    "Acceso a electricidad.csv": "electricity_access_total",
    "Access to electricity, rural.csv": "electricity_access_rural",
    "Access to electricity, urban.csv": "electricity_access_urban",
    "Consumo de energía eléctrica.csv": "electricity_consumption",
    "PIB por unidad de uso de energía.csv": "gdp_per_unit_energy_use",
    "Inversión en energía con particp privada.csv": "private_investment_energy",
    "Consumer Affordability of Electricity.csv": "consumer_affordability_electricity"
}

def leer_world_bank(ruta_archivo, nombre_variable):
    df = pd.read_csv(
        ruta_archivo,
        sep=";",
        engine="python",
        encoding="latin1",
        skiprows=4
    )

    df.columns = [str(col).strip().replace("\ufeff", "") for col in df.columns]
    df = df.rename(columns={"Country Name": "country"})

    if "country" not in df.columns:
        raise ValueError(f"No se encontró la columna 'Country Name' en {ruta_archivo}")

    df = df.drop(columns=["Country Code", "Indicator Name", "Indicator Code"], errors="ignore")

    df = df.melt(
        id_vars=["country"],
        var_name="year",
        value_name=nombre_variable
    )

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    return df

def leer_affordability(ruta_archivo, nombre_variable):
    df = pd.read_csv(
        ruta_archivo,
        sep=";",
        engine="python",
        encoding="latin1"
    )

    df.columns = [str(col).strip().replace("\ufeff", "") for col in df.columns]

    df = df.rename(columns={
        "REF_AREA_LABEL": "country",
        "TIME_PERIOD": "year",
        "OBS_VALUE": nombre_variable
    })

    df = df[["country", "year", nombre_variable]].copy()
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
            df = leer_world_bank(ruta_archivo, nombre_variable)

        lista_dfs.append(df)
        print(f"OK: {archivo}")

    except Exception as e:
        print(f"Error en {archivo}: {e}")

if len(lista_dfs) == 0:
    raise ValueError("No se ha podido cargar ningún dataset.")

df_electrificacion = lista_dfs[0]

for df in lista_dfs[1:]:
    df_electrificacion = df_electrificacion.merge(df, on=["country", "year"], how="outer")

print("\nPrimeras filas:")
print(df_electrificacion.head())

print("\nInformación general:")
print(df_electrificacion.info())

df_electrificacion.to_csv("dataset_electrificacion_unificado.csv", index=False)
print("\nArchivo guardado como: dataset_electrificacion_unificado.csv")