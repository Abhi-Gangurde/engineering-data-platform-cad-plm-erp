#!/bin/bash

# ==========================================
# 🔥 USAGE:
# ./setup_infra.sh <env> <location>
# Example:
# ./setup_infra.sh dev centralindia
# ==========================================

# -----------------------------
# INPUT PARAMETERS
# -----------------------------
ENV=$1 #dev #test #prod
LOCATION=$2 #centralindia

# -----------------------------
# VALIDATION
# -----------------------------
if [ -z "$ENV" ] || [ -z "$LOCATION" ]; then
  echo "❌ Usage: ./setup_infra.sh <env> <location>"
  exit 1
fi

# -----------------------------
# VARIABLES (DYNAMIC NAMING)
# -----------------------------
PROJECT="engdata"

RESOURCE_GROUP="rg-${PROJECT}-platform-${ENV}"
STORAGE_ACCOUNT="st${PROJECT}platform${ENV}"
ADF_NAME="adf-${PROJECT}-${ENV}"
KEYVAULT_NAME="kv-${PROJECT}-${ENV}"

# -----------------------------
# START SCRIPT
# -----------------------------
echo "🚀 Starting Infra Setup for ENV: $ENV"

# -----------------------------
# CREATE RESOURCE GROUP
# -----------------------------
echo "📦 Creating Resource Group: $RESOURCE_GROUP"

az group create \
  --name $RESOURCE_GROUP \
  --location $LOCATION

# -----------------------------
# CREATE STORAGE ACCOUNT
# -----------------------------
echo "💾 Creating Storage Account: $STORAGE_ACCOUNT"

az storage account create \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku Standard_LRS \
  --kind StorageV2

# -----------------------------
# CREATE CONTAINERS
# -----------------------------
echo "📁 Creating Containers..."

containers=("bronze" "silver" "gold" "serving" "metadata")

for container in "${containers[@]}"
do
    echo "Creating container: $container"

    az storage container create \
        --name $container \
        --account-name $STORAGE_ACCOUNT \
        --auth-mode login
done

# -----------------------------
# CREATE DATA FACTORY
# -----------------------------
echo "⚙️ Creating Data Factory: $ADF_NAME"

az datafactory create \
  --resource-group $RESOURCE_GROUP \
  --factory-name $ADF_NAME \
  --location $LOCATION

# -----------------------------
# CREATE KEY VAULT
# -----------------------------
echo "🔐 Creating Key Vault: $KEYVAULT_NAME"

az keyvault create \
  --name $KEYVAULT_NAME \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION

# -----------------------------
# DONE
# -----------------------------
echo "✅ Infra Setup Completed Successfully!"
echo "-------------------------------------"
echo "Resource Group: $RESOURCE_GROUP"
echo "Storage Account: $STORAGE_ACCOUNT"
echo "ADF: $ADF_NAME"
echo "Key Vault: $KEYVAULT_NAME"