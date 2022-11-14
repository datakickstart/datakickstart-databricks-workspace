# Databricks notebook source
def adls_authenticate(account_name="dvtrainingadls"):
  """
  Dependencies:`
    - Service principal is created and assigned permission
    - Secret scope created using Azure Key Vault or Databricks Secret Scope
    - Key values are added to the secret scope so that references from dbutils.secrets.get work properly
  """
  secret_scope_name = "demo"
  client_id = dbutils.secrets.get(secret_scope_name, f'{account_name}-client-id')
  app_id = dbutils.secrets.get(secret_scope_name, f'{account_name}-app-id')
  directory_id = dbutils.secrets.get(secret_scope_name, f'{account_name}-directory-id')
  credential = dbutils.secrets.get(secret_scope_name, f'{account_name}-credential')
  
  spark.conf.set("fs.azure.account.auth.type", "OAuth")
  spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
  spark.conf.set("fs.azure.account.oauth2.client.id", client_id)
  spark.conf.set("fs.azure.account.oauth2.client.secret", credential)
  spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/{0}/oauth2/token".format(directory_id))

# COMMAND ----------

def adls_mount(account_name="dvtrainingadls", container="raw", mnt_pnt="/mnt/datalake/raw"):
  """
  Dependencies:
    - Service principal is created and assigned permission
    - Secret scope created using Azure Key Vault or Databricks Secret Scope
    - Key values are added to the secret scope so that references from dbutils.secrets.get work properly
  """
  secret_scope_name = "demo"
  client_id = dbutils.secrets.get(secret_scope_name, 'dvtrainingadls-client-id')
  app_id = dbutils.secrets.get(secret_scope_name, 'dvtrainingadls-app-id')
  directory_id = dbutils.secrets.get(secret_scope_name, 'dvtrainingadls-directory-id')
  credential = dbutils.secrets.get(secret_scope_name, 'dvtrainingadls-credential')
  
  configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": client_id,
           "fs.azure.account.oauth2.client.secret": credential,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{0}/oauth2/token".format(directory_id)}

  # Optionally, you can add <directory-name> to the source URI of your mount point.
  try:
      dbutils.fs.mount(
        source = f"abfss://{container}@{account_name}.dfs.core.windows.net/",
        mount_point = mnt_pnt,
        extra_configs = configs
      )
  except Exception as e:
    if str(e).find("Directory already mounted") > -1:
        print(f"Skipping mount for {mnt_pnt}, mount already exists.")

# COMMAND ----------

def default_mounts():
    adls_mount(account_name="datakickstartadls", container="raw", mnt_pnt="/mnt/datalake/raw")
    adls_mount(account_name="datakickstartadls", container="refined", mnt_pnt="/mnt/datalake/refined")    
    adls_mount(account_name="datakickstartadls", container="curated", mnt_pnt="/mnt/datalake/curated")

# COMMAND ----------

# from datetime import datetime

# t = datetime.fromtimestamp(1657812704)
# print(t)
