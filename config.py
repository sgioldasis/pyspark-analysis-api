INPUT_PATH = "resources/data/input"
HOST = "127.0.0.1"
DATABASE = "db"
PORT = 3306
USER = "user"
PASSWORD = "password"
JDBC_URL = f"jdbc:mysql://localhost:{PORT}/{DATABASE}"
JDBC_DRIVER = "com.mysql.jdbc.Driver"
JDBC_JAR = "resources/jar/mysql-connector-java-8.0.19.jar"

DURATION_5_MINS = "5 minutes"
TAG_5_MINS = "5-minute"

DURATION_1_HOUR = "1 hour"
TAG_1_HOUR = "1-hour"

INTERVAL_5_MINS = (DURATION_5_MINS, TAG_5_MINS)
INTERVAL_1_HOUR = (DURATION_1_HOUR, TAG_1_HOUR)

KPI1_TABLE_NAME = "kpi1"
KPI1_COLUMNS = [
    "interval_start_timestamp",
    "interval_end_timestamp",
    "service_id",
    "total_bytes",
    "interval"
]

KPI2_TABLE_NAME = "kpi2"
KPI2_COLUMNS = [
    "interval_start_timestamp",
    "interval_end_timestamp",
    "cell_id",
    "number_of_unique_users",
    "interval"
]
