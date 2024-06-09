-- Create Table
CREATE TABLE IF NOT EXISTS
  lineage_data.lineagedemo.menu (
    recipe_id INT,
    app string,
    main string,
    dessert string
  );

-- Insert Data

INSERT INTO lineage_data.lineagedemo.menu
    (recipe_id, app, main, dessert)
VALUES
    (1,"Ceviche", "Tacos", "Flan"),
    (2,"Tomato Soup", "Souffle", "Creme Brulee"),
    (3,"Chips","Grilled Cheese","Cheesecake");


-- Create Table using Select
CREATE TABLE
  lineage_data.lineagedemo.dinner
AS SELECT
  recipe_id, concat(app," + ", main," + ",dessert)
AS
  full_menu
FROM
  lineage_data.lineagedemo.menu
