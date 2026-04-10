from brus_backend_common.models.lakehouse_model import DeltaModel

def migrate(model: DeltaModel):
    if model.spark:
        sql_string = f"""
            -- ALTER TABLE {model.TABLE_REF}
            -- DROP COLUMN test_column;
            
            CREATE TABLE {model.TABLE_REF}_copy AS
            SELECT * EXCEPT (test_column)
            FROM {model.TABLE_REF};
            
            DROP TABLE {model.TABLE_REF};
            
            ALTER TABLE {model.TABLE_REF}_copy RENAME TO {model.TABLE_REF};
        """
        model.spark.sql(sql_string)

def reverse_migrate(model: DeltaModel):
    if model.spark:
        sql_string = f"""
            ALTER TABLE {model.TABLE_REF}
            ADD COLUMNS (test_column STRING);
        """
        model.spark.sql(sql_string)
