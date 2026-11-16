CREATE CATALOG IF NOT EXISTS retail;
-- Si el metastore no tiene almacenamiento por defecto, indícalo:
-- CREATE CATALOG retail MANAGED LOCATION 's3://mi-bucket/retail';

USE CATALOG retail;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE VOLUME IF NOT EXISTS retail.bronze.raw;