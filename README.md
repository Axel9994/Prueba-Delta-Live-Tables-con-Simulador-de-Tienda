# Simulador de Tienda con Azure Event Hubs y Delta Live Tables

Este proyecto simula una tienda de víveres que envía datos de ventas en tiempo real a Azure Event Hubs y los procesa usando Delta Live Tables en Azure Databricks. Incluye scripts para desplegar la infraestructura, generar datos, procesarlos en un pipeline DLT y limpiar los recursos.

## Descripción

El proyecto consta de:
- **Generador de datos**: Un script Python que simula ventas y envía datos en formato JSON a Azure Event Hubs.
- **Infraestructura**: Un script de Azure CLI para desplegar Event Hubs y Databricks con un secreto configurado.
- **Pipeline DLT**: Un pipeline en Cuaderno de Python que procesa los datos en capas bronze, silver y gold
- **Limpieza**: Un script de Azure CLI para eliminar todos los recursos creados.

### Arquitectura
1. **Fuente de datos**: Simulador Python → Azure Event Hubs.
2. **Procesamiento**: Azure Databricks con Delta Live Tables.
3. **Capas**:
   - **Bronze**: Datos crudos en tabla sales_raw.
   - **Silver**: Datos limpios en tabla sales_silver.
   - **Gold**: Resumen diario por método de pago y producto en table sales_gold.

## Requisitos previos
- [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli) instalado y autenticado (`az login`).
- Suscripción de Azure con permisos para crear recursos (Event Hubs, Databricks, grupos de recursos).
- Python 3.8+ con las siguientes dependencias:
  ```bash
  pip install azure-eventhub



## Resultados

![Creacion de Canalización](https://images4.imagebam.com/ba/1a/e5/ME101M85_o.JPG)

![Dashboard Prueba](https://images4.imagebam.com/03/d9/e5/ME101M87_o.JPG)
