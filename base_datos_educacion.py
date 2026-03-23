import pandas as pd
import os

ruta = "Datos TFG/Educacion"

# Archivos educación csv
archivos_variables = {
    "Educational attainment rate.csv": "educational_attainment_rate",
    "Gasto por alumno primario.csv": "spending_per_student_primary",
    "Gasto por alumno secundario.csv": "spending_per_student_secondary",
    "Gasto público en educación.csv": "public_expenditure_education",
    "Inscripción escolar, nivel primario.csv": "school_enrollment_primary",
    "Inscripción escolar, nivel secundario .csv": "school_enrollment_secondary",
    "Lower secondary schools access to electricity.csv": "lower_secondary_schools_electricity",
    "Niños económicamente activos mujeres.csv": "economically_active_children_female",
    "Niños económicamente activos varones.csv": "economically_active_children_male",
    "Niños que no asisten mujeres.csv": "out_of_school_children_female",
    "Niños que no asisten varones.csv": "out_of_school_children_male",
    "Number of years of free primary and secondary education guaranteed.csv": "years_free_primary_secondary",
    "Primary completion rate.csv": "primary_completion_rate",
    "Primary schools Internet.csv": "primary_schools_internet",
    "Primary schools access to electricity (%).csv": "primary_schools_electricity",
    "Secondary schools Internet .csv": "secondary_schools_internet",
    "Tasa de alfabetización.csv": "literacy_rate",
    "Upper secondary schools access to electricity.csv": "upper_secondary_schools_electricity",
    "Years of compulsory education guaranteed.csv": "years_compulsory_education"
}

# Función para leer archivos de educación con estructura tipo World Bank
def leer_educacion_wb(ruta_archivo, nombre_variable):
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

    # Convertir valor a numérico
    df[nombre_variable] = pd.to_numeric(df[nombre_variable], errors="coerce")

    # Limpiar código país
    df["country_code"] = df["country_code"].astype(str).str.strip().str.upper()

    return df


# Función para leer archivos en formato largo tipo:
# indicatorId | geoUnit | year | value
def leer_educacion_corregido(ruta_archivo, nombre_variable):
    # probar primero con separador coma
    try:
        df = pd.read_csv(ruta_archivo)
    except:
        df = pd.read_csv(ruta_archivo, sep=";", engine="python", encoding="latin1")

    df.columns = [str(col).strip().replace("\ufeff", "") for col in df.columns]

    # Renombrar posibles columnas
    if "Country Code" in df.columns:
        df = df.rename(columns={"Country Code": "country_code"})
    if "geoUnit" in df.columns:
        df = df.rename(columns={"geoUnit": "country_code"})
    if "OBS_VALUE" in df.columns:
        df = df.rename(columns={"OBS_VALUE": nombre_variable})
    if "value" in df.columns:
        df = df.rename(columns={"value": nombre_variable})

    # Comprobar columnas mínimas
    if "country_code" not in df.columns:
        raise ValueError(f"No se encontró ni 'Country Code' ni 'geoUnit' en {ruta_archivo}")
    if "year" not in df.columns:
        raise ValueError(f"No se encontró la columna 'year' en {ruta_archivo}")
    if nombre_variable not in df.columns:
        raise ValueError(f"No se encontró la columna de valor en {ruta_archivo}")

    # Quedarnos solo con lo necesario
    df = df[["country_code", "year", nombre_variable]].copy()

    # Limpiar
    df["country_code"] = df["country_code"].astype(str).str.strip().str.upper()
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df[nombre_variable] = pd.to_numeric(df[nombre_variable], errors="coerce")

    df = df.dropna(subset=["country_code", "year"])
    df["year"] = df["year"].astype(int)

    return df


lista_dfs = []

for archivo, nombre_variable in archivos_variables.items():
    ruta_archivo = os.path.join(ruta, archivo)

    if not os.path.exists(ruta_archivo):
        print(f"No encontrado: {archivo}")
        continue

    try:
        # Primero intenta leer como World Bank
        try:
            df = leer_educacion_wb(ruta_archivo, nombre_variable)
        except:
            # Si falla, intenta como formato largo/corregido
            df = leer_educacion_corregido(ruta_archivo, nombre_variable)

        lista_dfs.append(df)
        print(f"OK: {archivo}")

    except Exception as e:
        print(f"Error en {archivo}: {e}")

if len(lista_dfs) == 0:
    raise ValueError("No se ha podido cargar ningún dataset de educación.")

# Unir todos los datasets por country_code y year
df_educacion = lista_dfs[0]

for df in lista_dfs[1:]:
    df_educacion = df_educacion.merge(df, on=["country_code", "year"], how="outer")

# Ordenar
df_educacion = df_educacion.sort_values(["country_code", "year"]).reset_index(drop=True)

# Revisar resultado
print("\nPrimeras filas:")
print(df_educacion.head())

print("\nInformación general:")
print(df_educacion.info())

# Guardar dataset final
df_educacion.to_csv("dataset_educacion_unificado.csv", index=False)
print("\nArchivo guardado como: dataset_educacion_unificado.csv")