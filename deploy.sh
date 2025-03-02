#!/bin/bash

# Variables de configuración
RESOURCE_GROUP="rg-databricks-eventhubs"
LOCATION="eastus"
EVENTHUB_NAMESPACE="ehns-demo-$(openssl rand -hex 4)"
EVENTHUB_NAME="eh-demo"
DATABRICKS_WORKSPACE="db-workspace-$(openssl rand -hex 4)"
SKU_EVENTHUB="Standard"
SKU_DATABRICKS="premium"
SECRET_SCOPE="eventhub-secrets"
SECRET_KEY="connectionString"

# Colores para la salida
GREEN='\033[0;32m'
NC='\033[0m'

# Crear grupo de recursos
echo -e "${GREEN}Creando grupo de recursos...${NC}"
az group create \
    --name $RESOURCE_GROUP \
    --location $LOCATION

# Crear namespace de Event Hubs
echo -e "${GREEN}Creando namespace de Event Hubs...${NC}"
az eventhubs namespace create \
    --resource-group $RESOURCE_GROUP \
    --name $EVENTHUB_NAMESPACE \
    --location $LOCATION \
    --sku $SKU_EVENTHUB \
    --enable-kafka true

# Crear Event Hub
echo -e "${GREEN}Creando Event Hub...${NC}"
az eventhubs eventhub create \
    --resource-group $RESOURCE_GROUP \
    --namespace-name $EVENTHUB_NAMESPACE \
    --name $EVENTHUB_NAME \
    --partition-count 4 \
    --retention-time 7

# Crear workspace de Databricks
echo -e "${GREEN}Creando workspace de Databricks...${NC}"
az databricks workspace create \
    --resource-group $RESOURCE_GROUP \
    --name $DATABRICKS_WORKSPACE \
    --location $LOCATION \
    --sku $SKU_DATABRICKS

# Obtener la cadena de conexión de Event Hubs
echo -e "${GREEN}Obteniendo cadena de conexión de Event Hubs...${NC}"
CONNECTION_STRING=$(az eventhubs namespace authorization-rule keys list \
    --resource-group $RESOURCE_GROUP \
    --namespace-name $EVENTHUB_NAMESPACE \
    --name RootManageSharedAccessKey \
    --query primaryConnectionString \
    --output tsv)

# Crear scope de secretos en Databricks
echo -e "${GREEN}Creando scope de secretos en Databricks...${NC}"
az databricks secret scope create \
    --name $SECRET_SCOPE \
    --workspace-name $DATABRICKS_WORKSPACE \
    --resource-group $RESOURCE_GROUP

# Guardar la connection string como secreto
echo -e "${GREEN}Guardando connection string en secreto...${NC}"
az databricks secret write \
    --scope $SECRET_SCOPE \
    --key $SECRET_KEY \
    --value "$CONNECTION_STRING" \
    --workspace-name $DATABRICKS_WORKSPACE \
    --resource-group $RESOURCE_GROUP

# Mostrar información importante
echo -e "${GREEN}Despliegue completado. Detalles importantes:${NC}"
echo "Resource Group: $RESOURCE_GROUP"
echo "Event Hubs Namespace: $EVENTHUB_NAMESPACE"
echo "Event Hub Name: $EVENTHUB_NAME"
echo "Databricks Workspace: $DATABRICKS_WORKSPACE"
echo "Secret Scope: $SECRET_SCOPE"
echo "Secret Key: $SECRET_KEY"
echo "Connection String stored in secret: $SECRET_SCOPE/$SECRET_KEY"

# Guardar información en un archivo
echo -e "${GREEN}Guardando información en config.txt${NC}"
cat > config.txt << EOF
RESOURCE_GROUP=$RESOURCE_GROUP
EVENTHUB_NAMESPACE=$EVENTHUB_NAMESPACE
EVENTHUB_NAME=$EVENTHUB_NAME
DATABRICKS_WORKSPACE=$DATABRICKS_WORKSPACE
SECRET_SCOPE=$SECRET_SCOPE
SECRET_KEY=$SECRET_KEY
EOF

# Instrucciones adicionales
echo -e "${GREEN}Pasos siguientes:${NC}"
echo "1. Usa el secreto en tu pipeline DLT: {{secrets/$SECRET_SCOPE/$SECRET_KEY}}"
echo "2. Configura permisos adicionales si es necesario en la UI de Databricks"
echo "3. Crea y ejecuta tu pipeline DLT"