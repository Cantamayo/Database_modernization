# 🚀 Proyecto de Modernización de Base de Datos: Migración a Snowflake

[![Author](https://img.shields.io/badge/Author-Christian_Andrey_Tamayo_Merchan-blue)]()
[![Bootcamp](https://img.shields.io/badge/Bootcamp-SoyHenry_Data_Science-yellow)]()
[![Stack](https://img.shields.io/badge/Stack-Python_%7C_PostgreSQL_%7C_Snowflake-success)]()

## 📋 Descripción del Proyecto
Este proyecto de modernización de infraestructura de datos (Proyecto M2) tiene como objetivo simular un entorno empresarial real donde se migra información desde un entorno local (On-Premise) hacia una arquitectura moderna en la nube. 

El flujo de trabajo abarca desde la generación de **500,000 registros sintéticos** sobre información de estudiantes, su inserción y modelado en un motor relacional **PostgreSQL**, hasta la construcción de un pipeline ETL automatizado en **Python** para cargar y disponibilizar estos datos en **Snowflake**.

## 🏗️ Arquitectura y Flujo de Trabajo
1. **Generación de Datos (`A1`):** Script de Python que crea medio millón de registros sintéticos, simulando una base de datos transaccional con información de estudiantes.
2. **Almacenamiento y Optimización On-Premise (`A2`):** Diseño del esquema en PostgreSQL, carga de datos y creación de índices para optimizar los tiempos de respuesta.
3. **Pipeline ETL y Migración Cloud (`A3`):** Extracción de los datos desde PostgreSQL, transformación ligera y carga (Load) automatizada hacia un Data Warehouse en Snowflake.
4. **Automatización Serverless (`A4`):** Integración de funciones Lambda para la ejecución de procesos específicos en la nube.

## 📂 Estructura del Repositorio

- **`Documentacion/`**
  - `Data_dictionary.xlsx`: Diccionario detallado de las variables y metadatos del proyecto.
  - `Manual Consultas SQL.pdf`: Documentación de las queries de análisis implementadas.
  - `avance #4.pdf`: Reporte de seguimiento del proyecto.

- **`Scripts/`**
  - `A1-01_data_generation_estudiantes.py`: Generador de los 500k registros sintéticos.
  - `02_queries_analysis.sql`: Consultas SQL para el análisis exploratorio de los datos.
  - `A2-03_optimization_indexes.sql`: Script para la creación de índices y optimización en PostgreSQL.
  - `04_dimensional_model.sql`: Creación del modelo dimensional (Tablas de Hechos y Dimensiones).
  - `A3-05_etl_pipeline_estudiantes.py`: Pipeline principal de migración hacia Snowflake.
  - `A4-lambda_functions.py`: Funciones de AWS Lambda para la automatización de procesos.

## 🛠️ Tecnologías Utilizadas
* **Lenguaje:** Python (Pandas, SQLAlchemy, Snowflake-connector)
* **Bases de Datos:** PostgreSQL (Local) / Snowflake (Cloud)
* **Cloud/Serverless:** AWS Lambda
* **Herramientas de Análisis:** SQL DDL/DML

## 🚀 Instrucciones de Ejecución
1. Clonar el repositorio.
2. Instalar las dependencias de Python (`pip install -r requirements.txt`).
3. Configurar las credenciales de PostgreSQL y Snowflake en un archivo `.env` local (no incluido en este repositorio por seguridad).
4. Ejecutar los scripts de la carpeta `Scripts/` en orden alfanumérico (A1 -> A2 -> A3...).