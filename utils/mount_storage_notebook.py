# Databricks notebook source
def adls_authenticate():
  """
  Dependencies:`
    - Service principal is created and assigned permission
    - Secret scope created using Azure Key Vault or Databricks Secret Scope
    - Key values are added to the secret scope so that references from dbutils.secrets.get work properly
  """
  secret_scope_name = "demo"
  account_name = "dvtrainingadls"
  client_id = dbutils.secrets.get(secret_scope_name, 'dvtrainingadls-client-id')
  app_id = dbutils.secrets.get(secret_scope_name, 'dvtrainingadls-app-id')
  directory_id = dbutils.secrets.get(secret_scope_name, 'dvtrainingadls-directory-id')
  credential = dbutils.secrets.get(secret_scope_name, 'dvtrainingadls-credential')
  
  spark.conf.set("fs.azure.account.auth.type", "OAuth")
  spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
  spark.conf.set("fs.azure.account.oauth2.client.id", client_id)
  spark.conf.set("fs.azure.account.oauth2.client.secret", credential)
  spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/{0}/oauth2/token".format(directory_id))

# COMMAND ----------

adls_authenticate()

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
  dbutils.fs.mount(
    source = f"abfss://{container}@{account_name}.dfs.core.windows.net/",
    mount_point = mnt_pnt,
    extra_configs = configs
  )
  

# COMMAND ----------

adls_mount(account_name="datakickstartadls", container="raw", mnt_pnt="/mnt/datalake/raw")

# COMMAND ----------

adls_mount(account_name="datakickstartadls", container="refined", mnt_pnt="/mnt/datalake/refined")

# COMMAND ----------

adls_mount(account_name="datakickstartadls", container="curated", mnt_pnt="/mnt/datalake/curated")

# COMMAND ----------

# MAGIC %sh rm -r /dbfs/tmp/stackoverflow/posts/_Post*

# COMMAND ----------

# MAGIC %sh du -h /dbfs/mnt/datalake/raw/stackoverflow/

# COMMAND ----------

adls_mount(account_name="dvtrainingadls", container="demo", mnt_pnt="/mnt/dvtraining/demo")

# COMMAND ----------

from datetime import datetime

t = datetime.fromtimestamp(1657812704)
print(t)
