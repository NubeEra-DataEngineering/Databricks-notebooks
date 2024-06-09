# 1. Create Resource Group

POSTFIX_PATTERN="304"
AZ_LOCATION="eastus"

AZ_RG_NAME="rg"$POSTFIX_PATTERN
AZ_IDENTITY_NAME="azidentity"$POSTFIX_PATTERN
AZ_DBWS_NAME="azdbws"$POSTFIX_PATTERN
AZ_STORAGE_ACCOUNT_NAME="azsa"$POSTFIX_PATTERN
AZ_CONTAINER_NAME="metastore"
AZ_DB_ACCESS_CONNECTER_NAME="access-connector"$POSTFIX_PATTERN

AZ_STORAGE_ACCOUNT_NAME="sa304"
AZ_CONTAINER_NAME="container"
AZ_DB_ACCESS_CONNECTER_NAME="acc304"


bash ```
az group create \
  --name $AZ_RG_NAME \ 
  --location $AZ_LOCATION
```

# 2. Create Databricks Workspace
bash ```
az databricks workspace create \ 
  --resource-group $AZ_RG_NAME \ 
  --name $AZ_DBWS_NAME \ 
  --location $AZ_LOCATION \ 
  --sku premium
```

3. Create a Storage Account
bash ```
az storage account create \
  --resource-group $AZ_RG_NAME --location $AZ_LOCATION \
  --name $AZ_STORAGE_ACCOUNT_NAME \
  --sku Standard_LRS \
  --kind StorageV2 --hns
```
az storage container create \
  --name $AZ_CONTAINER_NAME \
  --account-name $AZ_STORAGE_ACCOUNT_NAME


4. Create a User Assigned Managed Identity
az identity create \
  -g $AZ_RG_NAME \
  -n $AZ_IDENTITY_NAME

principalId=$(az identity show \
  --name $AZ_IDENTITY_NAME \
  --resource-group $AZ_RG_NAME \
  --query principalId \
  --output tsv \
)

scope=$(az storage account show \
  --name $AZ_STORAGE_ACCOUNT_NAME \
  --resource-group $AZ_RG_NAME \
  --query id --output tsv)

az role assignment create \
  --assignee $principalId \
  --role "Storage Blob Data Contributor" \
  --scope $scope

az role assignment create --assignee $principalId --role "Storage Queue Data Contributor" --scope $scope

principalResourceId=$(az identity show \
  --resource-group $AZ_RG_NAME \
  --name $AZ_IDENTITY_NAME \
  --query id \
  --output tsv)
  
# ex: /subscriptions/<<SUBID>>/resourcegroups/rg-db-exo/providers/Microsoft.ManagedIdentity/userAssignedIdentities/mi-db-exo


5. Create Access Connector Resource
===============================================================
az databricks access-connector create \
  --resource-group $AZ_RG_NAME \
  --name $AZ_DB_ACCESS_CONNECTER_NAME \
  --location $AZ_LOCATION \
  --identity-type UserAssigned \
  --user-assigned-identities "{$principalResourceId}"

  metastore@dbexostorageadlsv2.dfs.core.windows.net/

az databricks access-connector show \
  -n $AZ_DB_ACCESS_CONNECTER_NAME \
  --resource-group $AZ_RG_NAME \
  --query id

#output 
/subscriptions/<SUBID>/resourceGroups/rg-db-exo/providers/Microsoft.Databricks/accessConnectors/exo-db-access-connector

az identity show \
  --name $AZ_IDENTITY_NAME \
  --resource-group $AZ_RG_NAME \
  --query id

#output
/subscriptions/<SUBID>/resourcegroups/rg-db-exo/providers/Microsoft.ManagedIdentity/userAssignedIdentities/mi-db-exo


%sql
CREATE CATALOG IF NOT EXISTS exo_cat;
GRANT USE CATALOG ON CATALOG exo_cat TO `account users`;
USE CATALOG exo_cat;
CREATE SCHEMA IF NOT EXISTS patients;
CREATE VOLUME IF NOT EXISTS exo_cat.patients.fhir_vol;
DESCRIBE VOLUME exo_cat.patients.fhir_vol;

file_info_list = dbutils.fs.ls("/Volumes/exo_cat/patients/fhir_vol")
display(file_info_list)

%sql
SHOW STORAGE CREDENTIALS
