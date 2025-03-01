import random
import json
import time
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData
import os

# Configuración de Azure Event Hubs (reemplaza con tus propias credenciales)
EVENT_HUB_CONNECTION_STR = "Endpoint=sb://<tu-namespace>.servicebus.windows.net/;SharedAccessKeyName=<tu-key-name>;SharedAccessKey=<tu-key>;EntityPath=<tu-event-hub>"
EVENT_HUB_NAME = "<tu-event-hub-name>"

# Lista de productos de la tienda
PRODUCTOS = [
    {"id": 1, "nombre": "Arroz", "precio": 2.50, "categoria": "granos"},
    {"id": 2, "nombre": "Frijoles", "precio": 1.80, "categoria": "granos"},
    {"id": 3, "nombre": "Leche", "precio": 3.20, "categoria": "lacteos"},
    {"id": 4, "nombre": "Pan", "precio": 1.50, "categoria": "panaderia"},
    {"id": 5, "nombre": "Huevos", "precio": 4.00, "categoria": "proteinas"},
    {"id": 6, "nombre": "Azúcar", "precio": 2.00, "categoria": "dulces"}
]

# Métodos de pago
METODOS_PAGO = ["efectivo", "tarjeta", "transferencia"]

def generar_venta():
    """Genera una venta aleatoria"""
    cantidad_productos = random.randint(1, 5)
    productos_vendidos = random.sample(PRODUCTOS, cantidad_productos)
    
    venta = {
        "timestamp": datetime.now().isoformat(),
        "productos": [
            {
                "id": prod["id"],
                "nombre": prod["nombre"],
                "precio": prod["precio"],
                "cantidad": random.randint(1, 3)
            } for prod in productos_vendidos
        ],
        "metodo_pago": random.choice(METODOS_PAGO),
        "total": sum(prod["precio"] * random.randint(1, 3) for prod in productos_vendidos),
        "tienda_id": random.randint(1, 10)
    }
    return venta

def enviar_a_event_hub(producer, venta):
    """Envía la venta a Azure Event Hubs"""
    try:
        venta_json = json.dumps(venta)
        event_data = EventData(venta_json)
        producer.send_batch([event_data])
        print(f"Venta enviada en: {venta['timestamp']}")
    except Exception as e:
        print(f"Error al enviar a Event Hub: {str(e)}")

def main():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME
    )
    
    try:
        print("Simulador de tienda iniciado...")
        while True:
            venta = generar_venta()
            enviar_a_event_hub(producer, venta)
            tiempo_espera = random.uniform(1, 5)
            time.sleep(tiempo_espera)
            
    except KeyboardInterrupt:
        print("\nSimulación detenida por el usuario")
    finally:
        producer.close()

if __name__ == "__main__":
    if "Endpoint" in EVENT_HUB_CONNECTION_STR:
        print("Por favor, configura tus credenciales de Azure Event Hubs")
    else:
        main()