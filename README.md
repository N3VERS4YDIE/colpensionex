# Colpensionex

Este sistema automatiza diversas tareas programadas utilizando la biblioteca `schedule` de Python y programación multiproceso para una mayor eficiencia. Las tareas están configuradas para ejecutarse a intervalos específicos a lo largo del día.

## Descripción

El script realiza las siguientes acciones:

1. **Mover solicitudes a "en proceso"**:

   - Se ejecuta al inicio de cada hora (`:00`).

2. **Generar archivos CSV de entrada**:

   - Se ejecuta diariamente a las 8:15, 9:15, 10:15, 11:15, 14:15, 15:15, 16:15 y 17:15.

3. **Mover caracterizaciones a "en proceso"**:

   - Se ejecuta diariamente a las 00:30.

4. **Procesar solicitudes**:
   - Se ejecuta diariamente a la 01:00.

El script utiliza un bucle infinito para ejecutar las tareas pendientes en los horarios configurados.

## Requisitos

- Python 3.7 o superior

Puedes instalar los requerimientos ejecutando:

```bash
pip install -r requirements.txt
```

## Ejecutar

Para ejecutar el proyecto usa el comando:

```bash
python3 main.py
```
