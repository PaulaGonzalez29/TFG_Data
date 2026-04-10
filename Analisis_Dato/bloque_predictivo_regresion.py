import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split, cross_val_score, KFold
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import os
import warnings
warnings.filterwarnings('ignore')

#Carpeta en la que guardaremos los resultados del modelo 
os.makedirs('Analisis_Dato/ResultadosAnalisisPredictivo', exist_ok=True)
#Datos a aplicar en el modelo
df = pd.read_csv('Dataset_Final/dataset_limpio_2023.csv')

# 1. DEFINICIÓN DE VARIABLES
#Variable objetivo de la regresión lineal múltiple
TARGET = 'edu_educational_attainment_rate'

# Variables predictoras:
PREDICTORAS = [
    'elec_electricity_access_total', 
    'des_gdp_per_capita', #variable de control del nivel de desarrollo del país
    'des_tasa_mortalidad_infantil',
    'edu_public_expenditure_education',
    'edu_primary_completion_rate', #elegida como variable objetivo adicional
    'edu_primary_schools_electricity',
]

df_modelo = df[[TARGET] + PREDICTORAS].dropna()
print(f"Países con datos completos para el modelo: {len(df_modelo)} de {len(df)}")
print(f"Variable objetivo:  {TARGET}")
print(f"Variables predictoras ({len(PREDICTORAS)}): {PREDICTORAS}")

X = df_modelo[PREDICTORAS]
y = df_modelo[TARGET]

# 2. DIVISIÓN TRAIN / TEST
# Realizamos un división del dataset 80% para train y 20% para test
# Tenemos una muestra de 150 países, 30 se usarán para test
# Se mantiene también la división train/test para los gráficos.
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

print(f"\nDivisión Train/Test:")
print(f"  Train: {len(X_train)} países")
print(f"  Test:  {len(X_test)} países")

# 3. ENTRENAMIENTO DEL MODELO
modelo = LinearRegression()
modelo.fit(X_train, y_train)

print("\n=== MODELO A: REGRESIÓN LINEAL MÚLTIPLE ===")
print(f"Intercepto: {modelo.intercept_:.4f}")
print("\nCoeficientes:")
for var, coef in zip(PREDICTORAS, modelo.coef_):
    print(f"  {var:<45} {coef:+.4f}")

# 4. EVALUACIÓN DEL MODELO
# 4.1 Cross-validation
#División del dataset (5 divisiones)
kf = KFold(n_splits=5, shuffle=True, random_state=42)
#Ejecutamos 5 iteraciones, entrena con 4 grupos y evalúa con el 5º
cv_r2   = cross_val_score(modelo, X, y, cv=kf, scoring='r2')
cv_mae  = -cross_val_score(modelo, X, y, cv=kf,
                            scoring='neg_mean_absolute_error')
cv_rmse = np.sqrt(-cross_val_score(modelo, X, y, cv=kf,
                                    scoring='neg_mean_squared_error'))

print("\n=== MÉTRICAS — CROSS-VALIDATION ===")
print(f"{'Métrica':<10} {'Media':>10} {'Std':>10}")
print(f"{'R²':<10} {cv_r2.mean():>10.4f} {cv_r2.std():>10.4f}")
print(f"{'MAE':<10} {cv_mae.mean():>10.4f} {cv_mae.std():>10.4f}")
print(f"{'RMSE':<10} {cv_rmse.mean():>10.4f} {cv_rmse.std():>10.4f}")

# 4.2 Train / Test (Métricas)
y_pred_train = modelo.predict(X_train)
y_pred_test  = modelo.predict(X_test)

r2_train   = r2_score(y_train, y_pred_train)
r2_test    = r2_score(y_test,  y_pred_test)
mae_train  = mean_absolute_error(y_train, y_pred_train)
mae_test   = mean_absolute_error(y_test,  y_pred_test)
rmse_train = np.sqrt(mean_squared_error(y_train, y_pred_train))
rmse_test  = np.sqrt(mean_squared_error(y_test,  y_pred_test))

print("\n=== MÉTRICAS — TRAIN / TEST ===")
print(f"{'Métrica':<10} {'Train':>10} {'Test':>10}")
print(f"{'R²':<10} {r2_train:>10.4f} {r2_test:>10.4f}")
print(f"{'MAE':<10} {mae_train:>10.4f} {mae_test:>10.4f}")
print(f"{'RMSE':<10} {rmse_train:>10.4f} {rmse_test:>10.4f}")

# Guardar métricas
metricas = pd.DataFrame({
    'Métrica': ['R²', 'MAE', 'RMSE'],
    'CV Media': [cv_r2.mean(), cv_mae.mean(), cv_rmse.mean()],
    'CV Std':   [cv_r2.std(),  cv_mae.std(),  cv_rmse.std()],
    'Train':    [r2_train, mae_train, rmse_train],
    'Test':     [r2_test,  mae_test,  rmse_test],
}).round(4)
#Guardar métricas
metricas.to_csv(
    'Analisis_Dato/ResultadosAnalisisPredictivo/metricas_regresion.csv',
    index=False, encoding='utf-8-sig', sep=';', decimal=','
)
print("\nMétricas guardadas: metricas_regresion.csv")

# 5. ANÁLISIS DE MULTICOLINEALIDAD (VIF)
# Cálculo de VIF
print("\n=== FACTOR DE INFLACIÓN DE LA VARIANZA (VIF) ===")
print("(VIF > 10 indica multicolinealidad problemática)")

vif_resultados = []
for i, col in enumerate(X_train.columns):
    #Para calcularlo, seleccionamos todas las variables predictoras EXCEPTO la que estamos analizando
    otras = [c for c in X_train.columns if c != col]
    #Entrenamos la regresión con las variables predictoras
    r2_aux = r2_score(
        X_train[col],
        LinearRegression().fit(X_train[otras], X_train[col]).predict(X_train[otras])
    )
    #Fórmula de VIF = 1/(1-R^2)
    vif = 1 / (1 - r2_aux) if r2_aux < 1 else np.inf
    vif_resultados.append({'Variable': col, 'VIF': round(vif, 2)})

vif_df = pd.DataFrame(vif_resultados).sort_values('VIF', ascending=False)
print(vif_df.to_string(index=False))

vif_df.to_csv(
    'Analisis_Dato/ResultadosAnalisisPredictivo/vif_regresion.csv',
    index=False, encoding='utf-8-sig', sep=';', decimal=','
)
print("VIF guardado: vif_regresion.csv")

# 6. VISUALIZACIONES
# 6.1 Valores reales vs predichos (conjunto test)
plt.figure(figsize=(8, 6))
plt.scatter(y_test, y_pred_test,
            alpha=0.7, color='steelblue',
            edgecolors='white', linewidth=0.5, s=60,
            label='Países (test)')
lims = [min(y_test.min(), y_pred_test.min()) - 2,
        max(y_test.max(), y_pred_test.max()) + 2]
plt.plot(lims, lims, color='tomato', linewidth=2,
         linestyle='--', label='Predicción perfecta')
plt.xlabel('Valor Real — Logro Educativo (%)', fontsize=12)
plt.ylabel('Valor Predicho — Logro Educativo (%)', fontsize=12)
plt.title(
    f'Regresión Lineal — Real vs Predicho\n'
    f'R² CV = {cv_r2.mean():.3f} | R² test = {r2_test:.3f}',
    fontsize=12, fontweight='bold'
)
plt.legend()
plt.tight_layout()
plt.savefig('Analisis_Dato/ResultadosAnalisisPredictivo/rl_real_vs_predicho.png', dpi=150)
plt.close()
print("\nGráfico real vs predicho guardado: rl_real_vs_predicho.png")

# 6.2 Residuos vs valores predichos
residuos = y_test - y_pred_test

plt.figure(figsize=(8, 5))
plt.scatter(y_pred_test, residuos,
            alpha=0.7, color='steelblue',
            edgecolors='white', linewidth=0.5, s=60)
plt.axhline(y=0, color='tomato', linewidth=2, linestyle='--')
plt.xlabel('Valor Predicho', fontsize=12)
plt.ylabel('Residuo (Real − Predicho)', fontsize=12)
plt.title('Análisis de Residuos — Regresión Lineal',
          fontsize=12, fontweight='bold')
plt.tight_layout()
plt.savefig('Analisis_Dato/ResultadosAnalisisPredictivo/rl_residuos.png', dpi=150)
plt.close()
print("Gráfico de residuos guardado: rl_residuos.png")

# 6.3 Coeficientes del modelo
coef_df = pd.DataFrame({
    'Variable':    PREDICTORAS,
    'Coeficiente': modelo.coef_
}).sort_values('Coeficiente', key=abs, ascending=True)

colores_coef = ['tomato' if c < 0 else 'steelblue'
                for c in coef_df['Coeficiente']]

plt.figure(figsize=(9, 5))
bars = plt.barh(coef_df['Variable'], coef_df['Coeficiente'],
                color=colores_coef, edgecolor='white')
plt.axvline(x=0, color='black', linewidth=0.8)
for bar, val in zip(bars, coef_df['Coeficiente']):
    plt.text(
        val + (0.05 if val >= 0 else -0.05),
        bar.get_y() + bar.get_height() / 2,
        f'{val:+.3f}', va='center',
        ha='left' if val >= 0 else 'right', fontsize=9
    )
plt.xlabel('Coeficiente', fontsize=12)
plt.title(
    'Coeficientes — Regresión Lineal Múltiple\n'
    '(azul = efecto positivo | rojo = efecto negativo)',
    fontsize=12, fontweight='bold'
)
plt.tight_layout()
plt.savefig('Analisis_Dato/ResultadosAnalisisPredictivo/rl_coeficientes.png', dpi=150)
plt.close()
print("Gráfico de coeficientes guardado: rl_coeficientes.png")

# 6.4 R² por set de datos
plt.figure(figsize=(7, 4))
plt.bar([f'Fold {i+1}' for i in range(5)], cv_r2,
        color='steelblue', alpha=0.8, edgecolor='white')
plt.axhline(y=cv_r2.mean(), color='tomato', linewidth=2,
            linestyle='--', label=f'Media R² = {cv_r2.mean():.3f}')
plt.ylabel('R²', fontsize=12)
plt.title('R² por fragmento de dataset',
          fontsize=12, fontweight='bold')
plt.ylim(0, 1)
plt.legend()
plt.tight_layout()
plt.savefig('Analisis_Dato/ResultadosAnalisisPredictivo/rl_cv_r2.png', dpi=150)
plt.close()
print("Gráfico cross-validation guardado: rl_cv_r2.png")

print("\n Modelo A (Regresión Lineal Múltiple) completado.")

# 1. DEFINICIÓN DE VARIABLES
# Mismas variables que el Modelo A para que la comparación sea directa
TARGET = 'edu_educational_attainment_rate'

PREDICTORAS = [
    'elec_electricity_access_total',
    'des_gdp_per_capita',
    'des_tasa_mortalidad_infantil',
    'edu_public_expenditure_education',
    'edu_primary_completion_rate',
    'edu_primary_schools_electricity',
]
# Eliminamos los países que tengan algún nulo en estas variables
df_modelo = df[[TARGET] + PREDICTORAS].dropna()
print(f"Países con datos completos para el modelo: {len(df_modelo)} de {len(df)}")
print(f"Variable objetivo:  {TARGET}")
print(f"Variables predictoras ({len(PREDICTORAS)}): {PREDICTORAS}")

X = df_modelo[PREDICTORAS]
y = df_modelo[TARGET]

# 2. DIVISIÓN TRAIN / TEST
# Misma semilla que el Modelo A para garantizar
# que los conjuntos train/test son idénticos
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

print(f"\nDivisión Train/Test:")
print(f"  Train: {len(X_train)} países")
print(f"  Test:  {len(X_test)} países")

# 3. ENTRENAMIENTO DEL MODELO
# n_estimators: número de árboles
# max_depth: profundidad máxima de cada árbol
# min_samples_leaf: mínimo de países en cada hoja del árbol
# random_state: semilla para reproducibilidad
modelo_rf = RandomForestRegressor(
    n_estimators=200,
    max_depth=None,
    min_samples_leaf=3,
    random_state=42
)

modelo_rf.fit(X_train, y_train)
print("\n=== MODELO B: RANDOM FOREST REGRESSOR ===")
print(f"Número de árboles: {modelo_rf.n_estimators}")
print(f"Min muestras por hoja: {modelo_rf.min_samples_leaf}")

# 4. EVALUACIÓN DEL MODELO
# 4.1 Cross-validation 5 secciones
# Misma configuración que Modelo A para comparación directa
kf = KFold(n_splits=5, shuffle=True, random_state=42)

cv_r2   = cross_val_score(modelo_rf, X, y, cv=kf, scoring='r2')
cv_mae  = -cross_val_score(modelo_rf, X, y, cv=kf,
                            scoring='neg_mean_absolute_error')
cv_rmse = np.sqrt(-cross_val_score(modelo_rf, X, y, cv=kf,
                                    scoring='neg_mean_squared_error'))

print("\n=== MÉTRICAS — CROSS-VALIDATION 5 FOLDS (métrica principal) ===")
print(f"{'Métrica':<10} {'Media':>10} {'Std':>10}")
print(f"{'R²':<10} {cv_r2.mean():>10.4f} {cv_r2.std():>10.4f}")
print(f"{'MAE':<10} {cv_mae.mean():>10.4f} {cv_mae.std():>10.4f}")
print(f"{'RMSE':<10} {cv_rmse.mean():>10.4f} {cv_rmse.std():>10.4f}")

# 4.2 Train / Test (referencia complementaria)
y_pred_train = modelo_rf.predict(X_train)
y_pred_test  = modelo_rf.predict(X_test)

r2_train   = r2_score(y_train, y_pred_train)
r2_test    = r2_score(y_test,  y_pred_test)
mae_train  = mean_absolute_error(y_train, y_pred_train)
mae_test   = mean_absolute_error(y_test,  y_pred_test)
rmse_train = np.sqrt(mean_squared_error(y_train, y_pred_train))
rmse_test  = np.sqrt(mean_squared_error(y_test,  y_pred_test))

print("\n=== MÉTRICAS — TRAIN / TEST ===")
print(f"{'Métrica':<10} {'Train':>10} {'Test':>10}")
print(f"{'R²':<10} {r2_train:>10.4f} {r2_test:>10.4f}")
print(f"{'MAE':<10} {mae_train:>10.4f} {mae_test:>10.4f}")
print(f"{'RMSE':<10} {rmse_train:>10.4f} {rmse_test:>10.4f}")

# Guardar métricas
metricas_rf = pd.DataFrame({
    'Métrica': ['R²', 'MAE', 'RMSE'],
    'CV Media': [cv_r2.mean(), cv_mae.mean(), cv_rmse.mean()],
    'CV Std':   [cv_r2.std(),  cv_mae.std(),  cv_rmse.std()],
    'Train':    [r2_train, mae_train, rmse_train],
    'Test':     [r2_test,  mae_test,  rmse_test],
}).round(4)
metricas_rf.to_csv(
    'Analisis_Dato/ResultadosAnalisisPredictivo/metricas_random_forest.csv',
    index=False, encoding='utf-8-sig', sep=';', decimal=','
)
print("\nMétricas guardadas: metricas_random_forest.csv")


# 5. IMPORTANCIA DE VARIABLES (Feature Importance)
# El feature importance indica qué variables tienen más peso
# en las decisiones del modelo (¿qué factores explican más el logro educativo?)
importancias = pd.DataFrame({
    'Variable':    PREDICTORAS,
    'Importancia': modelo_rf.feature_importances_
}).sort_values('Importancia', ascending=False).round(4)

print("\n=== IMPORTANCIA DE VARIABLES (Feature Importance) ===")
print(importancias.to_string(index=False))

importancias.to_csv(
    'Analisis_Dato/ResultadosAnalisisPredictivo/feature_importance_rf.csv',
    index=False, encoding='utf-8-sig', sep=';', decimal=','
)
print("Feature importance guardado: feature_importance_rf.csv")

# 6. COMPARATIVA MODELOS A vs B
# Cargamos métricas del Modelo A para comparar
metricas_rl = pd.read_csv(
    'Analisis_Dato/ResultadosAnalisisPredictivo/metricas_regresion.csv',
    sep=';', decimal=','
)

print("\n=== COMPARATIVA MODELO A (Regresión Lineal) vs MODELO B (Random Forest) ===")
print(f"{'Métrica':<10} {'RL CV Media':>14} {'RF CV Media':>14} {'Mejora':>10}")
for _, row in metricas_rl.iterrows():
    metrica = row['Métrica']
    val_rl  = row['CV Media']
    val_rf  = metricas_rf[metricas_rf['Métrica'] == metrica]['CV Media'].values[0]
    # Para R² mayor es mejor; para MAE y RMSE menor es mejor
    if metrica == 'R²':
        mejora = val_rf - val_rl
        mejor  = 'RF ✓' if mejora > 0 else 'RL ✓'
    else:
        mejora = val_rl - val_rf
        mejor  = 'RF ✓' if mejora > 0 else 'RL ✓'
    print(f"{metrica:<10} {val_rl:>14.4f} {val_rf:>14.4f} {mejor:>10}")

comparativa = pd.DataFrame({
    'Métrica':      metricas_rl['Métrica'],
    'RL CV Media':  metricas_rl['CV Media'],
    'RF CV Media':  metricas_rf['CV Media'],
    'RL CV Std':    metricas_rl['CV Std'],
    'RF CV Std':    metricas_rf['CV Std'],
}).round(4)
comparativa.to_csv(
    'Analisis_Dato/ResultadosAnalisisPredictivo/comparativa_modelos.csv',
    index=False, encoding='utf-8-sig', sep=';', decimal=','
)
print("\nComparativa guardada: comparativa_modelos.csv")

# 7. VISUALIZACIONES
# 7.1 Valores reales vs predichos
plt.figure(figsize=(8, 6))
plt.scatter(y_test, y_pred_test,
            alpha=0.7, color='seagreen',
            edgecolors='white', linewidth=0.5, s=60,
            label='Países (test)')
lims = [min(y_test.min(), y_pred_test.min()) - 2,
        max(y_test.max(), y_pred_test.max()) + 2]
plt.plot(lims, lims, color='tomato', linewidth=2,
         linestyle='--', label='Predicción perfecta')
plt.xlabel('Valor Real — Logro Educativo (%)', fontsize=12)
plt.ylabel('Valor Predicho — Logro Educativo (%)', fontsize=12)
plt.title(
    f'Random Forest — Real vs Predicho\n'
    f'R² CV = {cv_r2.mean():.3f} | R² test = {r2_test:.3f}',
    fontsize=12, fontweight='bold'
)
plt.legend()
plt.tight_layout()
plt.savefig('Analisis_Dato/ResultadosAnalisisPredictivo/rf_real_vs_predicho.png', dpi=150)
plt.close()
print("\nGráfico real vs predicho guardado: rf_real_vs_predicho.png")

# 7.2 Residuos vs valores predichos
residuos = y_test - y_pred_test

plt.figure(figsize=(8, 5))
plt.scatter(y_pred_test, residuos,
            alpha=0.7, color='seagreen',
            edgecolors='white', linewidth=0.5, s=60)
plt.axhline(y=0, color='tomato', linewidth=2, linestyle='--')
plt.xlabel('Valor Predicho', fontsize=12)
plt.ylabel('Residuo (Real − Predicho)', fontsize=12)
plt.title('Análisis de Residuos — Random Forest',
          fontsize=12, fontweight='bold')
plt.tight_layout()
plt.savefig('Analisis_Dato/ResultadosAnalisisPredictivo/rf_residuos.png', dpi=150)
plt.close()
print("Gráfico de residuos guardado: rf_residuos.png")

# 7.3 Feature Importance
importancias_plot = importancias.sort_values('Importancia', ascending=True)

plt.figure(figsize=(9, 5))
bars = plt.barh(
    importancias_plot['Variable'],
    importancias_plot['Importancia'],
    color='seagreen', alpha=0.85, edgecolor='white'
)
for bar, val in zip(bars, importancias_plot['Importancia']):
    plt.text(
        val + 0.003,
        bar.get_y() + bar.get_height() / 2,
        f'{val:.3f}', va='center', ha='left', fontsize=9
    )
plt.xlabel('Importancia relativa', fontsize=12)
plt.title('Importancia de Variables — Random Forest\n'
          '(cuanto mayor, más relevante para predecir el logro educativo)',
          fontsize=12, fontweight='bold')
plt.tight_layout()
plt.savefig('Analisis_Dato/ResultadosAnalisisPredictivo/rf_feature_importance.png', dpi=150)
plt.close()
print("Feature importance guardado: rf_feature_importance.png")

# 7.4 R² por sección del dataset — comparativa RL vs RF
metricas_rl_cv = metricas_rl[metricas_rl['Métrica'] == 'R²']['CV Media'].values[0]

plt.figure(figsize=(7, 4))
plt.bar([f'Fold {i+1}' for i in range(5)], cv_r2,
        color='seagreen', alpha=0.8, edgecolor='white', label='Random Forest')
plt.axhline(y=cv_r2.mean(), color='seagreen', linewidth=2,
            linestyle='--', label=f'RF Media = {cv_r2.mean():.3f}')
plt.axhline(y=metricas_rl_cv, color='steelblue', linewidth=2,
            linestyle='--', label=f'RL Media = {metricas_rl_cv:.3f}')
plt.ylabel('R²', fontsize=12)
plt.title('R² por sección — Random Forest vs Regresión Lineal',
          fontsize=12, fontweight='bold')
plt.ylim(0, 1)
plt.legend()
plt.tight_layout()
plt.savefig('Analisis_Dato/ResultadosAnalisisPredictivo/rf_cv_r2.png', dpi=150)
plt.close()
print("Gráfico cross-validation guardado: rf_cv_r2.png")

print("\n Modelo B (Random Forest Regressor) completado.")