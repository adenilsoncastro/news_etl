import requests
import pandas as pd
import sqlalchemy
import sqlite3
import uuid
from dotenv import load_dotenv
from os import getenv

load_dotenv('.env')

DATABASE_LOCATION = '/path/to/database'
DATABASE_ENGINE = f'sqlite:///{DATABASE_LOCATION}'
API_KEY = getenv('NEWS_KEY')
COUNTRY = getenv('NEWS_COUNTRY')


def extract() -> pd.DataFrame:

    r = requests.get(
        f'https://newsapi.org/v2/top-headlines?country={COUNTRY}&apiKey={API_KEY}')
    data = r.json()

    authors = []
    titles = []
    descriptions = []
    urls = []
    urls_to_image = []
    publish_at = []
    ids = []

    if data['status'] == 'ok':
        for article in data['articles']:
            authors.append(article['author'])
            titles.append(article['title'])
            descriptions.append(article['description'])
            urls.append(article['url'])
            urls_to_image.append(article['urlToImage'])
            publish_at.append(article['publishedAt'])
            ids.append(str(uuid.uuid1()).split('-')[0])

    news_dict = {
        'id': ids,
        'author': authors,
        'title': titles,
        'description': descriptions,
        'url': urls,
        'url_to_image': urls_to_image,
        'published_at': publish_at
    }

    news_df = pd.DataFrame(news_dict, columns=[
                           'id', 'author', 'title', 'description', 'url', 'url_to_image', 'published_at'])

    return news_df


def transform(news_df: pd.DataFrame):
    # Apply some transformation here

    return news_df


def load(news_df: pd.DataFrame) -> bool:
    print(DATABASE_LOCATION)

    sqlite_connection = sqlite3.connect(DATABASE_LOCATION)
    cursor = sqlite_connection.cursor()
    engine = sqlalchemy.create_engine(DATABASE_ENGINE)

    sql_query = """
        CREATE TABLE IF NOT EXISTS news(
            id VARCHAR(32),
            author VARCHAR(200),
            title VARCHAR(1000),
            description VARCHAR(1000),
            url VARCHAR(500),
            url_to_image VARCHAR(500),
            published_at VARCHAR(100),
            CONSTRAINT primary_key_constraint PRIMARY KEY (id)
        )
        """
    try:
        cursor.execute(sql_query)
        print('Successfully connected to database')

        news_df.to_sql('news', engine, index=False, if_exists='append')

        sqlite_connection.close()
        return True

    except Exception as err:
        raise Exception(f'Database error:{err}')


def run_news_etl() -> None:
    news_df = extract()
    news_df_transformed = transform(news_df)
    result = load(news_df_transformed)


def main() -> None:
    print('Starting ETL')
    run_news_etl()


if __name__ == '__main__':
    main()
