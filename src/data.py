import mysql.connector
from flask import jsonify
from dateutil import parser


# Connect to the MySQL database
def dbconnect():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="password",
        database="fca")


def log_to_db(action, parameter, status):
    db = dbconnect()
    cursor = db.cursor()

    insert_query = "INSERT INTO log (action, parameter, status) VALUES (%s, %s, %s);"
    cursor.execute(insert_query, (action, parameter, status))
    db.commit()

    cursor.close()
    db.close()


def get_log(start, end):
    db = dbconnect()
    cursor = db.cursor()

    start_date = None
    end_date = None

    # check start date is valid
    if start is not None:
        start_date = parser.parse(start)
        start += " 00:00:00"

    # check end date is valid
    if end is not None:
        end_date = parser.parse(end)
        end += " 23:59:59"

    if start_date is not None and end_date is not None and end_date < start_date:
        raise ValueError

    try:
        select_query = "SELECT date, action, parameter, status FROM log "

        if start_date is not None and end_date is not None:
            select_query += "WHERE date >= '" + start + "' AND date <= '" + end + "' "
        elif start_date is not None and end_date is None:
            select_query += "WHERE date >= '" + start + "' "
        elif start_date is None and end_date is not None:
            select_query += "WHERE date <= '" + end + "' "

        select_query += "ORDER BY date"
        cursor.execute(select_query)

        # Get column names, need this so we can add the keys for the JSON
        columns = [col[0] for col in cursor.description]

        # Fetch all rows from the recordset
        rows = cursor.fetchall()

        # Convert each row into a dictionary with column names as keys
        logs = []
        for row in rows:
            record = dict(zip(columns, row))
            logs.append(record)
    finally:
        cursor.close()
        db.close()

    log_to_db("GET LOGS", "", "SUCCESS")
    return jsonify(logs)


def get_stat():
    db = dbconnect()
    cursor = db.cursor()

    try:
        backup_count_query = "SELECT COUNT(*) FROM log WHERE action='BACKUP'"
        cursor.execute(backup_count_query)
        number_of_backups = cursor.fetchone()

        success_count_query = "SELECT COUNT(*) FROM log WHERE status='SUCCESS' AND action='BACKUP'"  # noqa: E501
        cursor.execute(success_count_query)
        number_of_successes = cursor.fetchone()

        error_count_query = "SELECT COUNT(*) FROM log WHERE status='ERROR' AND action='BACKUP'"  # noqa: E501
        cursor.execute(error_count_query)
        number_of_errors = cursor.fetchone()
    finally:
        cursor.close()
        db.close()

    log_to_db("GET STATS", "", "SUCCESS")
    return jsonify({
        "number-of-backups": number_of_backups[0],
        "successful-backups": number_of_successes[0],
        "failed-backups": number_of_errors[0]
    })
