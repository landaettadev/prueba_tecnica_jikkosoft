#!/bin/bash
set -e

echo "=== Verificando entorno ==="
pwd
echo "Contenido del directorio actual:"
ls -la

echo "Contenido del directorio datasets:"
ls -la /app/datasets || echo "No se puede acceder a /app/datasets"

echo "=== Verificando archivos de dataset ==="
if [ -d "/app/datasets" ]; then
    for file in /app/datasets/dataset-*.txt; do
        if [ -f "$file" ]; then
            echo "Archivo: $file"
            echo "Líneas: $(wc -l < "$file")"
            echo "Tamaño: $(du -h "$file" | cut -f1)"
            echo "Primeras líneas:"
            head -n 2 "$file"
            echo "---"
        fi
    done
else
    echo "El directorio /app/datasets no existe"
fi

echo "=== Verificando variables de entorno ==="
env | grep -E "DB_|DATA_"

echo "=== Iniciando pipeline ==="
python pipelinecarga.py 