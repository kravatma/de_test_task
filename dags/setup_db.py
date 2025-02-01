import duckdb


if __name__ == "__main__":
    con = duckdb.connect("test.db")

    con.sql("CREATE SCHEMA IF NOT EXISTS test")
    con.sql("USE test.test")

    con.sql("""CREATE TABLE IF NOT EXISTS posts 
                    (user_id INTEGER,
                     id INTEGER,
                     title VARCHAR,
                     body VARCHAR
                    )
            """
            )

    con.sql("""CREATE TABLE IF NOT EXISTS comments
                        (post_id INTEGER,
                         id INTEGER,
                         name VARCHAR,
                         email VARCHAR,
                         body VARCHAR
                        )
            """
            )
    con.commit()

    print(con.sql("select table_name, database_name, schema_name from duckdb_tables"))
