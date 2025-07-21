from scipy.stats import poisson, norm
from scipy import stats
import gdown
import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


def descargarYConfigurarBaseDatos(id_archivo="1_8pUfdjv1VmxdSNyeI4DDfKiGRKbLHQf", archivo_salida="olist.sqlite"):
    """Descargar el archivo de base de datos y establecer conexión."""
    gdown.download(f"https://drive.google.com/uc?id={id_archivo}", archivo_salida, quiet=False)
    conexion = sqlite3.connect(archivo_salida)
    return conexion


def obtenerEstadisticasVentas(conexion, tabla_items="order_items", tabla_orders="orders",
                             campo_fecha="order_purchase_timestamp", estado_filtro="delivered"):
    """Obtener días totales y ventas de la base de datos."""
    cursor = conexion.cursor()
    consulta = f"""
        SELECT
        COUNT(DISTINCT date(o.{campo_fecha})) AS total_dias,
        COUNT(oi.order_item_id) AS cantidad_total_ventas
        FROM {tabla_items} oi
        JOIN {tabla_orders} o ON oi.order_id = o.order_id
        WHERE o.order_status = "{estado_filtro}";
    """

    cursor.execute(consulta)
    resultado = cursor.fetchone()

    dias_totales = resultado[0]
    ventas_totales = resultado[1]
    valor_lambda = ventas_totales / dias_totales

    print(f"Días totales: {dias_totales}")
    print(f"Ventas totales: {ventas_totales}")
    print(f"Lambda (λ): {valor_lambda}")

    # En distribución de Poisson: media = varianza = λ
    print(f"\n=== ESTADÍSTICAS PARA VARIABLE DISCRETA (Poisson) ===")
    print(f"Media: {valor_lambda:.4f}")
    print(f"Varianza: {valor_lambda:.4f}")
    print(f"Desviación estándar: {np.sqrt(valor_lambda):.4f}")

    return dias_totales, ventas_totales, valor_lambda


def calcularDistribucionPoisson(valor_lambda, valor_maximo=400):
    """Calcular probabilidades de distribución de Poisson."""
    valores = [x for x in range(valor_maximo + 1)]
    probabilidades = [poisson.pmf(x, valor_lambda) for x in valores]
    return valores, probabilidades


def graficarDistribucionPoisson(valores, probabilidades, valor_lambda, titulo="Distribución de Poisson"):
    """Graficar la distribución de Poisson con línea de moda."""
    # Encontrar la moda
    probabilidad_maxima = max(probabilidades)
    moda = valores[probabilidades.index(probabilidad_maxima)]

    # Crear gráfico
    plt.figure(figsize=(12, 6))
    plt.bar(valores, probabilidades, color='skyblue', label=f'X ~ Poisson(λ = {valor_lambda:.2f})')

    # Línea de moda
    plt.axvline(moda, color='red', linestyle='--', linewidth=2, label=f'Moda = {moda}')

    # Detalles del gráfico
    plt.title(titulo)
    plt.xlabel('Valores')
    plt.ylabel('Probabilidad P(X = x)')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.legend()
    plt.tight_layout()
    plt.show()

    print(f"Probabilidad total (suma de PMF): {sum(probabilidades):.6f}")
    print(f"Moda: {moda} con P = {probabilidad_maxima:.6f}")

    return moda, probabilidad_maxima


def crearTablaFrecuenciaVentas(conexion, tamano_intervalo=50, tabla_items="order_items",
                              tabla_orders="orders", campo_fecha="order_purchase_timestamp",
                              estado_filtro="delivered"):
    """Crear tabla de frecuencias agrupando ventas por intervalos."""
    # Consulta para obtener ventas por día
    consulta = f"""
    SELECT
        date(o.{campo_fecha}) AS fecha,
        COUNT(oi.order_item_id) AS ventas_dia
    FROM {tabla_items} oi
    JOIN {tabla_orders} o ON oi.order_id = o.order_id
    WHERE o.order_status = '{estado_filtro}'
    GROUP BY fecha
    """

    # Crear DataFrame con resultados
    df_ventas_por_dia = pd.read_sql_query(consulta, conexion)

    # Definir intervalos
    ventas_minimas = df_ventas_por_dia['ventas_dia'].min()
    ventas_maximas = df_ventas_por_dia['ventas_dia'].max()
    intervalos = range(ventas_minimas, ventas_maximas + tamano_intervalo, tamano_intervalo)

    # Crear etiquetas como "150-199", "200-249", etc.
    etiquetas = [f'{i}-{i+tamano_intervalo-1}' for i in intervalos[:-1]]

    # Clasificar cada día en rangos
    df_ventas_por_dia['rango_ventas'] = pd.cut(
        df_ventas_por_dia['ventas_dia'],
        bins=intervalos,
        labels=etiquetas,
        right=False
    )

    # Contar cuántos días están en cada rango
    tabla_frecuencias_rangos = (df_ventas_por_dia['rango_ventas']
                               .value_counts()
                               .sort_index()
                               .reset_index())
    tabla_frecuencias_rangos.columns = ['rango_ventas', 'frecuencia']

    print("Tabla de frecuencias de ventas por rangos:")
    print(tabla_frecuencias_rangos)

    return tabla_frecuencias_rangos


def obtenerValoresPedidos(conexion, tabla_items="order_items", tabla_orders="orders",
                         estado_filtro="delivered"):
    """Obtener los valores totales de pedidos de la base de datos."""
    consulta = f"""
    SELECT
        oi.order_id,
        SUM(oi.price + oi.freight_value) AS valor_total_pedido
    FROM {tabla_items} oi
    JOIN {tabla_orders} o ON oi.order_id = o.order_id
    WHERE o.order_status = '{estado_filtro}'
    GROUP BY oi.order_id
    """

    df_valor_pedidos = pd.read_sql_query(consulta, conexion)
    return df_valor_pedidos['valor_total_pedido']


def calcularFrecuenciaRelativa(valores, limite_superior):
    """Calcular la frecuencia relativa para valores menores o iguales a un límite."""
    valores_en_rango = valores[valores <= limite_superior]
    frecuencia_relativa = len(valores_en_rango) / len(valores)

    print(f"Cantidad de valores menores o iguales a {limite_superior}: {len(valores_en_rango)}")
    print(f"Frecuencia relativa para valores entre 0 y {limite_superior}: {frecuencia_relativa:.4f}")

    return frecuencia_relativa, len(valores_en_rango)


def filtrarOutliersPorPercentil(valores, percentil=100):
    """Filtrar outliers usando un percentil específico."""
    limite_percentil = valores.quantile(percentil/100)
    valores_filtrados = valores[valores <= limite_percentil]

    # Información sobre el filtro aplicado
    valores_eliminados = len(valores) - len(valores_filtrados)
    porcentaje_eliminado = (valores_eliminados / len(valores)) * 100

    print(f"\n=== FILTRO APLICADO (Percentil {percentil}%) ===")
    print(f"Límite percentil {percentil}%: {limite_percentil:.2f}")
    print(f"Valores originales: {len(valores)}")
    print(f"Valores después del filtro: {len(valores_filtrados)}")
    print(f"Valores eliminados (outliers): {valores_eliminados} ({porcentaje_eliminado:.1f}%)")
    print(f"Valor máximo original: {valores.max():.2f}")
    print(f"Valor máximo filtrado: {valores_filtrados.max():.2f}")

    return valores_filtrados, limite_percentil


def calcularEstadisticasDescriptivas(valores, nombre_variable="Valores", unidad=""):
    """Calcular estadísticas descriptivas de una serie de valores."""
    media = valores.mean()
    desviacion = valores.std()
    mediana = valores.median()
    valor_minimo = valores.min()
    valor_maximo = valores.max()

    print(f"\n=== ESTADÍSTICAS DESCRIPTIVAS ({nombre_variable}) ===")
    print(f"Media: {unidad}{media:.2f}")
    print(f"Desviación estándar: {unidad}{desviacion:.2f}")
    print(f"Mediana: {unidad}{mediana:.2f}")
    print(f"Valor mínimo: {unidad}{valor_minimo:.2f}")
    print(f"Valor máximo: {unidad}{valor_maximo:.2f}")
    print(f"Cantidad de valores analizados: {len(valores)}")

    # Calcular percentiles para mejor comprensión
    percentil25 = valores.quantile(0.25)
    percentil75 = valores.quantile(0.75)
    percentil95 = valores.quantile(0.95)

    print(f"Percentil 25%: {unidad}{percentil25:.2f}")
    print(f"Percentil 75%: {unidad}{percentil75:.2f}")
    print(f"Percentil 95%: {unidad}{percentil95:.2f}")

    # Coeficiente de variación
    coef_variacion = (desviacion / media) * 100
    print(f"Coeficiente de variación: {coef_variacion:.1f}%")

    return {
        'media': media,
        'desviacion': desviacion,
        'mediana': mediana,
        'minimo': valor_minimo,
        'maximo': valor_maximo,
        'percentil_25': percentil25,
        'percentil_75': percentil75,
        'percentil_95': percentil95,
        'coef_variacion': coef_variacion
    }


def analizarValorPedidos(conexion, percentil_filtro=100, limite_frecuencia_relativa=500,
                        tabla_items="order_items", tabla_orders="orders", estado_filtro="delivered"):
    """Analizar el valor de pedidos como variable continua con parámetros configurables."""

    # Obtener valores de pedidos
    valores_pedidos_originales = obtenerValoresPedidos(conexion, tabla_items, tabla_orders, estado_filtro)

    # Calcular frecuencia relativa
    frecuencia_relativa, cantidad_en_rango = calcularFrecuenciaRelativa(
        valores_pedidos_originales, limite_frecuencia_relativa
    )

    # Filtrar outliers por percentil
    valores_pedidos_filtrados, limite_percentil = filtrarOutliersPorPercentil(
        valores_pedidos_originales, percentil_filtro
    )

    # Calcular estadísticas descriptivas
    estadisticas = calcularEstadisticasDescriptivas(
        valores_pedidos_filtrados, "Valor de Pedidos Filtrado", "R$ "
    )

    return valores_pedidos_filtrados, estadisticas, frecuencia_relativa


def graficarDistribucionValores(valores, estadisticas, titulo="Distribución de Valores",
                               xlabel="Valores", unidad="", metodo_intervalos="sturges"):
    """Graficar histograma de valores con curva de densidad."""

    # Calcular número de intervalos según método elegido
    if metodo_intervalos == "sturges":
        numero_intervalos = int(np.ceil(np.log2(len(valores)) + 1))
    elif metodo_intervalos == "scott":
        numero_intervalos = int(np.ceil(2 * len(valores)**(1/3)))
    else:
        numero_intervalos = metodo_intervalos  # Si es un número específico

    # Crear gráfico con tamaño adecuado
    plt.figure(figsize=(12, 8))

    # Histograma normalizado (densidad)
    conteos, intervalos, barras = plt.hist(valores, bins=numero_intervalos,
                                          density=True, alpha=0.7, color='lightblue',
                                          edgecolor='black', label='Histograma')

    # Curva de densidad suavizada usando kernel density estimation
    densidad = stats.gaussian_kde(valores)
    x = np.linspace(valores.min(), valores.max(), 1000)
    plt.plot(x, densidad(x), 'r-', linewidth=2, label='Curva de Densidad')

    # Líneas de referencia para media y mediana
    plt.axvline(estadisticas['media'], color='red', linestyle='--', linewidth=2,
                label=f'Media = {unidad}{estadisticas["media"]:.2f}')
    plt.axvline(estadisticas['mediana'], color='green', linestyle='--',
                linewidth=2, label=f'Mediana = {unidad}{estadisticas["mediana"]:.2f}')

    # Configuración del gráfico
    plt.title(titulo, fontsize=14, fontweight='bold')
    plt.xlabel(xlabel, fontsize=12)
    plt.ylabel('Densidad', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=10)

    # Formatear eje x si hay unidad monetaria
    if unidad:
        plt.gca().xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{unidad}{x:.0f}'))

    plt.tight_layout()
    plt.show()

    # Información adicional sobre los intervalos y distribución
    print(f"\nInformación del histograma:")
    print(f"Método de intervalos: {metodo_intervalos}")
    print(f"Número de intervalos: {numero_intervalos}")
    if len(intervalos) > 1:
        print(f"Ancho promedio de intervalo: {unidad}{(intervalos[1] - intervalos[0]):.2f}")

    return conteos, intervalos


def main():
    """Función main para ejecutar el análisis completo con parámetros configurables."""
    print("=== Análisis de Datos de Ventas (Versión Parametrizada) ===\n")

    # Parámetros configurables
    PERCENTIL_FILTRO = 100
    LIMITE_FRECUENCIA_RELATIVA = 500
    TAMANO_INTERVALO_VENTAS = 50
    VALOR_MAXIMO_POISSON = 400

    # 1. Configurar base de datos
    print("1. Descargando y configurando base de datos...")
    conexion = descargarYConfigurarBaseDatos()

    # 2. Análisis de distribución de Poisson
    print("\n2. Análisis de Distribución de Poisson:")
    dias_totales, ventas_totales, valor_lambda = obtenerEstadisticasVentas(conexion)

    valores, probabilidades = calcularDistribucionPoisson(valor_lambda, VALOR_MAXIMO_POISSON)
    moda, probabilidad_maxima = graficarDistribucionPoisson(
        valores, probabilidades, valor_lambda, "Distribución de Poisson - Ventas por Día"
    )

    # 3. Análisis de tabla de frecuencias
    print("\n3. Análisis de Tabla de Frecuencias de Ventas:")
    tabla_frecuencias = crearTablaFrecuenciaVentas(conexion, TAMANO_INTERVALO_VENTAS)

    # 4. Análisis de valor de pedidos (con parámetros configurables)
    print(f"\n4. Análisis del Valor de Pedidos (FILTRADO POR PERCENTIL {PERCENTIL_FILTRO}%):")
    valores_pedidos, estadisticas, frecuencia_relativa = analizarValorPedidos(
        conexion, PERCENTIL_FILTRO, LIMITE_FRECUENCIA_RELATIVA
    )

    conteos, intervalos = graficarDistribucionValores(
        valores_pedidos, estadisticas,
        "Distribución del Valor de Pedidos",
        "Valor del Pedido", "R$ "
    )

    # Cerrar conexión de base de datos
    conexion.close()
    print("\n¡Análisis completado!")


if __name__ == "__main__":
    main()