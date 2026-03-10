FROM apache/airflow:2.8.1
RUN pip install --no-cache-dir openai slack-sdk psycopg2-binary pandas requests python-dotenv beautifulsoup4
