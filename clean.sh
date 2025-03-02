#!/bin/bash

# Variables de configuración (deben coincidir con el despliegue original)
RESOURCE_GROUP="rg-databricks-eventhubs"

# Colores para la salida
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Verificar si el grupo de recursos existe
echo -e "${GREEN}Verificando existencia del grupo de recursos...${NC}"
if az group exists --name $RESOURCE_GROUP; then
    echo -e "${GREEN}Grupo de recursos $RESOURCE_GROUP encontrado. Procediendo con la limpieza...${NC}"

    # Eliminar el grupo de recursos y todos sus recursos
    echo -e "${GREEN}Eliminando grupo de recursos $RESOURCE_GROUP y todos sus recursos...${NC}"
    az group delete \
        --name $RESOURCE_GROUP \
        --yes \
        --no-wait

    echo -e "${GREEN}Solicitud de eliminación enviada. La limpieza está en progreso en segundo plano.${NC}"
    echo "Puedes verificar el estado con: az group show --name $RESOURCE_GROUP"
else
    echo -e "${RED}El grupo de recursos $RESOURCE_GROUP no existe. No hay nada que limpiar.${NC}"
    exit 1
fi

# Limpiar archivo de configuración local (si existe)
if [ -f config.txt ]; then
    echo -e "${GREEN}Eliminando archivo de configuración local (config.txt)...${NC}"
    rm config.txt
fi

echo -e "${GREEN}Proceso de limpieza completado.${NC}"