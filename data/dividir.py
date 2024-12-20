import pandas as pd
import os

def dividir_csv(archivo_csv, tamano_maximo_mb=90, salida_directorio='data'):
    # Crear directorio de salida si no existe
    if not os.path.exists(salida_directorio):
        os.makedirs(salida_directorio)
    
    # Leer el CSV en trozos
    tamano_maximo_bytes = tamano_maximo_mb * 1024 * 1024
    chunk_size = 100000  # Número aproximado de filas a leer por chunk (ajustable)
    contador_parte = 1
    acumulado = 0
    
    for chunk in pd.read_csv(archivo_csv, chunksize=chunk_size, encoding='latin1'):
        # Calcular el tamaño del chunk actual
        chunk_bytes = chunk.memory_usage(deep=True).sum()
        acumulado += chunk_bytes

        # Escribir en un nuevo archivo si el acumulado supera el tamaño máximo permitido
        if acumulado > tamano_maximo_bytes:
            acumulado = chunk_bytes
            contador_parte += 1
        
        archivo_salida = os.path.join(salida_directorio, f'parte_{contador_parte}.csv')
        # Escribir los datos
        if not os.path.exists(archivo_salida):
            chunk.to_csv(archivo_salida, index=False, mode='w')
        else:
            chunk.to_csv(archivo_salida, index=False, mode='a', header=False)

    print(f"Archivo dividido en data de ~{tamano_maximo_mb} MB en la carpeta '{salida_directorio}'.")

# Usar la función
archivo_csv = 'training.1600000.processed.noemoticon.csv'
dividir_csv(archivo_csv)
