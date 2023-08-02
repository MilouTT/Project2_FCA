from flask import Flask, request
from backup import backup_folder
from data import get_log, get_stat

app = Flask(__name__)

@app.route('/')
def homepage():
    return '<h1>Home Page</h1>''<h2>Backup Application</h2>'

# Declares the route of where this function will perform
# curl localhost:5000/logs --> trigger the function
@app.route('/log')
def log():
    try:
        start = request.args.get('start')
        end = request.args.get('end')

        logs = get_log(start, end)
        response = logs
        return response
    except ValueError:
        response = "Invalid Dates"
        return response, 400


@app.route('/stat')
def stat():
    stats = get_stat()
    response = stats
    return response


@app.route('/', methods=['POST'])
def backup():  # put application's code here

    folder_to_backup = request.json["path"]
    try:
        if folder_to_backup is None:
            response = "No folder path supplied"
            return response, 400

        backup_folder(folder_to_backup)
        response = "Backup Completed"
        return response, 201
    except ValueError:
        response = "No such folder: " + folder_to_backup
        return response, 404


# Run Flask
if __name__ == '__main__':
    app.run('0.0.0.0')
