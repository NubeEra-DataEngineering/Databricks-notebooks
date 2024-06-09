# 0. Configure Environment Variables
####################################
POSTFIX_PATTERN="304"
AZ_LOCATION="eastus"

AZ_RG_NAME="rg"$POSTFIX_PATTERN
AZ_IDENTITY_NAME="azidentity"$POSTFIX_PATTERN
AZ_DBWS_NAME="azdbws"$POSTFIX_PATTERN
AZ_STORAGE_ACCOUNT_NAME="azsa"$POSTFIX_PATTERN
AZ_CONTAINER_NAME="metastore"
AZ_DB_ACCESS_CONNECTER_NAME="access-connector"$POSTFIX_PATTERN

# 1. Create Resource Group
###########################
az group create \
  --name $AZ_RG_NAME \
  --location $AZ_LOCATION

# 2. Create a Storage Account
#############################
az storage account create \
  --resource-group $AZ_RG_NAME \
  --location $AZ_LOCATION \
  --name $AZ_STORAGE_ACCOUNT_NAME \
  --sku Standard_LRS \
  --kind StorageV2 --hns

# 3. Create Container
#####################
az storage container create \
  --name $AZ_CONTAINER_NAME \
  --account-name $AZ_STORAGE_ACCOUNT_NAME

# 4. Create a User Assigned Managed Identity
############################################
az identity create \
  -g $AZ_RG_NAME \
  -n $AZ_IDENTITY_NAME

# 5. Create Role Assignment (Get Principal ID, Scope)
#####################################################
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

az role assignment create \
  --assignee $principalId \
  --role "Storage Queue Data Contributor" \
  --scope $scope

# 6. Get Principal Resource ID
#####################################
principalResourceId=$(az identity show \
  --resource-group $AZ_RG_NAME \
  --name $AZ_IDENTITY_NAME \
  --query id \
  --output tsv)
  
# ex: /subscriptions/<<SUBID>>/resourcegroups/rg-db-exo/providers/Microsoft.ManagedIdentity/userAssignedIdentities/mi-db-exo

#7. Create Access Connector Resource
####################################
az databricks access-connector create \
  --resource-group $AZ_RG_NAME \
  --name $AZ_DB_ACCESS_CONNECTER_NAME \
  --location $AZ_LOCATION \
  --identity-type UserAssigned \
  --user-assigned-identities "{$principalResourceId}"

# 8. Get Access Connector ID & Identity ID
##########################################
az databricks access-connector show \
  -n $AZ_DB_ACCESS_CONNECTER_NAME \
  --resource-group $AZ_RG_NAME \
  --query id

#output 
# /subscriptions/<SUBID>/resourceGroups/rg-db-exo/providers/Microsoft.Databricks/accessConnectors/exo-db-access-connector
# /subscriptions/fca36345-619f-4ce5-a80e-61941df69f35/resourceGroups/rg304/providers/Microsoft.Databricks/accessConnectors/access-connector304

az identity show \
  --name $AZ_IDENTITY_NAME \
  --resource-group $AZ_RG_NAME \
  --query id

# Output
# /subscriptions/<SUBID>/resourcegroups/rg-db-exo/providers/Microsoft.ManagedIdentity/userAssignedIdentities/mi-db-exo
# /subscriptions/fca36345-619f-4ce5-a80e-61941df69f35/resourcegroups/rg304/providers/Microsoft.ManagedIdentity/userAssignedIdentities/azidentity304

# 9. Create Databricks Workspace
################################
az databricks workspace create \ 
  --resource-group $AZ_RG_NAME \ 
  --name $AZ_DBWS_NAME \ 
  --location $AZ_LOCATION \ 
  --sku premium

# 10. In Databricks Workspace 
      #10.1 Delete if Already Exisits & Create Metastore
        # AZ_CONTAINER_NAME@AZ_STORAGE_ACCOUNT_NAME.dfs.core.windows.net/
        # Attach Workspace to MetaStore
        # Launch Workspace: https://adb-3831012682118653.13.azuredatabricks.net/aad/auth
      #10.2 Connect to Git Repo( https://oauth:TOKEN@github.com/NubeEra-DataEngineering/Databricks-notebooks.git  )
        #  ghp_YOUR_GITHUB_TOKEN_gh
      #10.3 Cluster with JSON File
      #10.4 Attach Existing Notebook
      #10.5 Attach Cluster to Notebook)
#######################################
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
