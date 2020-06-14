"""
This module provides the API functionality
"""

from flask import Flask
from flask_mysqldb import MySQL
import json
from datetime import datetime
from flask import request
import time
import config

# App Configuration
app = Flask(__name__)

app.config['MYSQL_USER'] = config.USER
app.config['MYSQL_PASSWORD'] = config.PASSWORD
app.config['MYSQL_HOST'] = config.HOST
app.config['MYSQL_DB'] = config.DATABASE
app.config['MYSQL_CURSORCLASS'] = 'DictCursor'

mysql = MySQL(app)


def sql_condition():
    """
    Returns the SQL condition corresponding to the request query string
    """

    # Get parameters from query string
    args = request.args
    for k, v in args.items():
        print(f"arg [{k}] = [{v}]")

    # Construct a list of SQL conditions based on query string parameters
    conditions = []
    if "from" in args:
        value = args["from"]
        dt_value = datetime.strptime(value, '%Y-%m-%d_%H:%M:%S')
        unixtime = int(time.mktime(dt_value.timetuple()) * 1000)
        print('converted', value, dt_value, unixtime)
        conditions.append(f"(interval_start_timestamp >= {unixtime})")
    if "to" in args:
        value = args["to"]
        dt_value = datetime.strptime(value, '%Y-%m-%d_%H:%M:%S')
        unixtime = int(time.mktime(dt_value.timetuple()) * 1000)
        print('converted', value, dt_value, unixtime)
        conditions.append(f"(interval_end_timestamp <= {unixtime})")
    if "interval" in args:
        value = args["interval"]
        conditions.append(f"(`interval` = '{value}')")

    # Construct the final condition (join the conditions in the list with AND)
    condition = "" if not conditions else " WHERE "+" AND ".join(conditions)
    print("condition", condition)

    # Return the final condition
    return condition


def db_results(sql):
    """
    Runs the provided sql statement, and fetches the results in JSON format
    """

    # Execute the SQL on the database and fetch the results
    cur = mysql.connection.cursor()
    cur.execute(sql)
    results = cur.fetchall()

    # Create output_rows list (transformed results for output)
    output_rows = []
    for result in results:
        result['interval_start_timestamp'] = datetime.fromtimestamp(
            result['interval_start_timestamp']/1000).strftime('%Y-%m-%d %H:%M:%S')
        result['interval_end_timestamp'] = datetime.fromtimestamp(
            result['interval_end_timestamp']/1000).strftime('%Y-%m-%d %H:%M:%S')
        output_rows.append(result)

    # Return json (based on transformed results)
    return json.dumps(output_rows)


@app.route('/kpi1/', methods=['GET'])
def get_kpi1():
    """
    Queries kpi1 database table and returns results in JSON format
    """

    # Construct the SQL statement to run on the database
    sql = f'''
      SELECT * 
      FROM kpi1 
      {sql_condition()}
      ORDER BY 
        `interval`,
        interval_start_timestamp, 
        interval_end_timestamp, 
        total_bytes desc,
        service_id desc
    '''
    print("sql", sql)

    # Run the SQL statement on the database and return results
    return db_results(sql)


@app.route('/kpi2/', methods=['GET'])
def get_kpi2():
    """
    Queries kpi2 database table and returns results in JSON format
    """

    # Construct the SQL statement to run on the database
    sql = f'''
      SELECT * 
      FROM kpi2
      {sql_condition()}
      ORDER BY 
        `interval`,
        interval_start_timestamp, 
        interval_end_timestamp, 
        number_of_unique_users desc,
        cell_id desc
    '''
    print("sql", sql)

    # Run the SQL statement on the database and return results
    return db_results(sql)
