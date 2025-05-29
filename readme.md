# Pipeline de Procesamiento de Consumos

## ¿Qué hace este proyecto?
Basicamente, toma una cantidad de archivos de consumo y los procesa de manera eficiente. Lo mas importante es que lo hace en pequeños lotes para no saturar la memoria.

### Caracteristicas principales:
- Procesa archivos en micro-lotes de hasta 1000 registros
- Detecta automáticamente el delimitador de los archivos 
- Calcula tasas basadas en tarifas por destino
- Maneja valores mínimos y maximos por año
- Todo se guarda en PostgreSQL para consultas rápidas

## ¿Cómo lo hice funcionar?
Se implemento:
- Un sistema de logging que te dice que esta pasando
- Manejo de errores robusto 
- Procesamiento en lotes para no acabar la memoria.
- Una tabla de estadisticas que se actualiza en tiempo real

## Configuración

### Opción 1: Instalación Local
Necesitas:
- Python 3.10+
- PostgreSQL
- Un archivo `.env` con tus credenciales (te dejo un ejemplo abajo)
 ```env
DB_USER=tu_usuario
DB_PASSWORD=tu_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=consumos_db
```

### Si usas instalación local:
1. Clona el repo
2. Instala las dependencias: `pip install -r requirements.txt`
3. Configura tu `.env`
4. Ejecuta: `python pipelinecarga.py`


### Opción 2: Usando Docker 🐳
Docker:
1. Asegúrate de tener Docker y Docker Compose instalados
2. Clona el repo
3. Ejecuta:
```bash
docker-compose up --build
```
El pipeline se ejecutará automáticamente con:
- PostgreSQL corriendo en un contenedor
- La aplicación Python en otro contenedor
- Todo configurado y listo para usar

## Estructura de archivos
Los archivos de entrada estan en la carpeta `datasets` con:
- `tarifa_por_destino.csv`: Tarifas por tipo de destino
- `minimos.csv`: Valores mínimos por año
- `maximos.csv`: Valores máximos por año
- Archivos `dataset-*.txt`: Tus datos de consumo
