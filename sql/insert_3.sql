INSERT INTO my_table VALUES (%(filename)s)
ON CONFLICT (table_value)
DO NOTHING