-- Databricks notebook source
--- Show all catalogs in the metastore
SHOW CATALOGS;

-- COMMAND ----------

--- create a new catalog (optional)
CREATE CATALOG IF NOT EXISTS lineage_data;

-- COMMAND ----------

-- Set the current catalog
USE CATALOG lineage_data;

-- COMMAND ----------

--- Check grants on the lineage_data catalog
SHOW GRANTS ON CATALOG lineage_data;

-- COMMAND ----------

--- Make sure that all required users have USE CATALOG priviledges on the catalog. 
--- In this example, we grant the priviledge to all account users.
GRANT USE CATALOG
ON CATALOG lineage_data
TO `account users`;

-- COMMAND ----------

-- Show schemas in the selected catalog
SHOW SCHEMAS;

-- COMMAND ----------

-- Create Schema
CREATE SCHEMA lineage_data.lineagedemo;

-- COMMAND ----------

--- Make sure that all required users have USE CATALOG priviledges on the catalog. 
--- In this example, we grant the priviledge to all account users.
GRANT USAGE, CREATE ON SCHEMA lineage_data.lineagedemo TO `account users`;

-- COMMAND ----------

-- Create Table
CREATE TABLE IF NOT EXISTS
  lineage_data.lineagedemo.menu (
    recipe_id INT,
    app string,
    main string,
    dessert string
  );

-- COMMAND ----------

-- Insert Data

INSERT INTO lineage_data.lineagedemo.menu
    (recipe_id, app, main, dessert)
VALUES
    (1,"Ceviche", "Tacos", "Flan"),
    (2,"Tomato Soup", "Souffle", "Creme Brulee"),
    (3,"Chips","Grilled Cheese","Cheesecake");

-- COMMAND ----------

-- Create Table using Select
CREATE TABLE
  lineage_data.lineagedemo.dinner
AS SELECT
  recipe_id, concat(app," + ", main," + ",dessert)
AS
  full_menu
FROM
  lineage_data.lineagedemo.menu


-- COMMAND ----------


