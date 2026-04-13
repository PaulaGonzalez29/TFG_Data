# ELECTRIFICACIÓN Y OPORTUNIDADES EDUCATIVAS: El impacto del acceso a electricidad en el desarrollo social
### Trabajo de Fin de Grado — Business Analytics | Universidad Francisco de Vitoria
### PAULA GONZÁLEZ RODRÍGUEZ

Análisis del impacto del acceso a la electricidad en el desarrollo educativo 
a escala global, utilizando datos de 205 países para el año 2023.

## Estructura del repositorio
CODIGO_TFG/
│
├── Datos TFG/                          # Archivos CSV originales descargados de las fuentes
│
├── Dataset_Final/                      # Datasets generados a lo largo del pipeline
│   ├── dataset_unificado.csv           # Dataset histórico completo (todos los años)
│   ├── dataset_transversal_2023.csv    # Corte transversal filtrado a 2023
│   ├── dataset_limpio_2023.csv         # Dataset final tras limpieza e imputación
│   ├── dataset_pca_2023.csv            # Dataset preparado para PCA (sin nulos)
│
├── Analisis_Calidad/                   # Outputs del análisis de calidad
│   ├── resumen_calidad_variables.csv
│   ├── resumen_calidad_variables_Limpio.csv
│   ├── resumen_nulos_paises.csv
│   ├── resumen_nulos_paises_Limpio.csv
│   ├── correlaciones_altas.csv
│   └── correlaciones_altas_Limpio.csv
│
├── Analisis_Dato/                      # Scripts y resultados del análisis del dato
│   ├── bloque_exploratorio.py          # PCA, K-Means y clusterización jerárquica
│   ├── bloque_predictivo_regresion.py  # Regresión lineal y Random Forest
│   ├── ResultadosAnalisisExploratorio/ # Gráficos y tablas del bloque exploratorio
│   └── ResultadosAnalisisPredictivo/   # Métricas, coeficientes y gráficos predictivos
│
├── base_datos_educacion.py             # Extracción e integración de variables educativas
├── base_datos_Electrif.py             # Extracción e integración de variables de electrificación
├── base_datos_desarrollo.py            # Extracción e integración de variables de desarrollo
├── dataset_hdi.py                      # Limpieza y normalización del dataset HDI (PNUD)
├── unificar_dataset.py                 # Integración global de los tres bloques temáticos
├── dataset_paises.py                   # Filtrado al año 2023 y construcción del corte transversal
├── analisis_calidad.py                 # Análisis de calidad antes y después de la limpieza
├── limpieza_datos.py                   # Limpieza, imputación y preparación del dataset final
├── eda.py                              # Análisis Exploratorio de Datos (EDA)
└── README.md
