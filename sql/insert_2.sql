INSERT INTO my_table VALUES ('{{ params.filename }}')
ON CONFLICT (table_value)
DO NOTHING