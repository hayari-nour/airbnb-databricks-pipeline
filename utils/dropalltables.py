# Lister toutes les tables dans le schema silver
tabless = [t.tableName for t in spark.sql("SHOW TABLES FROM workspace.silver").collect()]

# Supprimer toutes les tables
for t in tabless:
    spark.sql(f"DROP TABLE IF EXISTS workspace.silver.{t}")


# Lister toutes les tables dans le schema silver
tablesb = [t.tableName for t in spark.sql("SHOW TABLES FROM workspace.bronze").collect()]

# Supprimer toutes les tables
for t in tablesb:
    spark.sql(f"DROP TABLE IF EXISTS workspace.bronze.{t}")

