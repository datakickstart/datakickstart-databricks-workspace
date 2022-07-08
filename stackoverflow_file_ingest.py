# Databricks notebook source
# MAGIC %sh wget https://archive.org/download/stackexchange/stackoverflow.com-Users.7z -O /tmp/Users.7z

# COMMAND ----------

# MAGIC %sh mv /tmp/Users.yz /dbfs/tmp/Users.7z

# COMMAND ----------

# MAGIC %pip install py7zr

# COMMAND ----------

import py7zr
with py7zr.SevenZipFile('/dbfs/tmp/Users.7z', mode='r') as z:
    z.extractall(path='/dbfs/tmp')

# COMMAND ----------

#%sh p7zip -d /dbfs/tmp/Users.7z

# COMMAND ----------

# MAGIC %sh wget https://archive.org/download/stackexchange/stackoverflow.com-Posts.7z -O /tmp/Posts.7z

# COMMAND ----------

url = 'https://archive.org/download/stackexchange/stackoverflow.com-Posts.7z'
local_filename =  'posts.7z'
with requests.get(url, stream=True) as r:
    r.raise_for_status()
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
