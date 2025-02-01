import duckdb
import requests
import json
import pandas
from airflow.operators.python import PythonOperator
from airflow import DAG
from inflection import underscore
from airflow.utils.dates import days_ago, timedelta


con = duckdb.connect("test.db")
con.sql("USE test.test")


def update_table(table):
    """
    API не предусматривет инкрементальную загрузку,
    поэтому нужно каждый раз полностью очищать таблицу

    :param table:
    :return:
    """
    r = requests.get(f'https://jsonplaceholder.typicode.com/{table}')
    if r.status_code == 200:
        df = pandas.DataFrame(json.loads(r.text))
        # приведение названий столбцов к snake_case
        df.rename(columns=lambda x: underscore, inplace=True)
        con.execute(f"TRUNCATE TABLE {table}")
        con.execute(f"INSERT INTO {table} SELECT * FROM df")
        con.commit()
        inserted_rows = con.sql(f"select count(*) from {table}").fetchall()[0][0]
        assert inserted_rows == len(df), f"Потеряны строки при вставке в таблицу {table}"
    else:
        raise Exception(f"Ошибка получения данных из /{table}, код: {r.status_code}")


def update_posts():
    update_table('posts')


def update_comments():
    update_table('comments')


def update_final():
    con.execute("DROP TABLE IF EXISTS posts_with_comments")
    con.execute("""
                CREATE TABLE posts_with_comments AS
                SELECT p.user_id,
                       p.id as post_id,
                       p.title as post_title,
                       p.body as post_body,
                       c.id as comment_id,
                       c.name as comment_name,
                       c.email as comment_email,
                       c.body as comment_body
                FROM posts p
                LEFT JOIN comments c ON p.id = c.post_id
                """)
    con.commit()


args = {
    'owner': 'A.Kravchenko',
    'start_date': days_ago(1),
    'retry_delay': timedelta(minutes=5),
    'retries': 2,
}


with DAG(
        dag_id="task1_etl",
        description="Обработка постов и комментариев",
        default_args=args,
        schedule_interval="@daily",
        catchup=False,
        max_active_runs=1
        ) as dag:
    update_posts = PythonOperator(update_posts, task_id='update_posts')
    update_comments = PythonOperator(update_comments, task_id='update_comments')
    update_final = PythonOperator(update_final, task_id='update_final')

    update_posts >> update_final
    update_comments >> update_final


if __name__ == '__main__':
    # для локальной отладки
    update_posts()
    update_comments()
    update_final()
