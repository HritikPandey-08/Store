from flask import Flask, request, jsonify, Response
import uuid
import psycopg2
import csv
from datetime import datetime, timedelta, time
import pytz

app = Flask(__name__)

# Connect to the PostgreSQL database
conn = psycopg2.connect(database="backend_api", user="postgres", password="1234", host="localhost", port="5432")

# Open the CSV file and create a writer
csvfile = open('uptime_downtime.csv', 'w', newline='')
fieldnames = ['store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week', 'downtime_last_hour', 'downtime_last_day', 'downtime_last_week']
csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

# Write the header
csv_writer.writeheader()

# Generate a unique report_id
report_id = str(uuid.uuid4())
print("Report ID for further use:", report_id)

# Initialize Flask app
app = Flask(__name__)

# Dictionary to store the status of each report
report_status = {}

def calculate_uptime_downtime(store_id,conn):
    # Connect to your postgres DB
    try:
        cur = conn.cursor()

        # Getting store's timezone
        cur.execute(f"SELECT timezone_str FROM store_report_app_storetimezone WHERE store_id = {store_id};")
        timezone_row = cur.fetchone()

        # No timezone found, assume it as America/Chicago
        if timezone_row is None:
            print(f"No timezone found for store_id {store_id}. Using default timezone 'America/Chicago'.")
            timezone_str = 'America/Chicago'
        else:
            timezone_str = timezone_row[0]

        # Get the store's business hours
        cur.execute(f"SELECT day_of_week, start_time_local, end_time_local FROM store_report_app_storehours WHERE store_id = {store_id};")
        business_hours = cur.fetchall()

        # If no business hours are found, assume the store is open 24/7
        if not business_hours:
            business_hours = [(day, time(0, 0), time(23, 59)) for day in range(7)]

        # Get the store's status data
        cur.execute(f"SELECT timestamp_utc, status FROM store_report_app_storestatus WHERE store_id = {store_id} ORDER BY timestamp_utc;")
        status_data = cur.fetchall()
        
        # Get the current timestamp
        cur.execute(f"SELECT MAX(timestamp_utc) FROM store_report_app_storestatus;")
        max_timestamp = cur.fetchone()

        # Initialize uptime and downtime counters
        uptime_last_hour = 0
        uptime_last_day = 0
        uptime_last_week = 0
        downtime_last_hour = 0
        downtime_last_day = 0
        downtime_last_week = 0

        # Convert business hours to datetime objects
        business_hours = [(day, start, end) for day, start, end in business_hours]
    
        for timestamp_utc, status in status_data:
            timestamp_local = timestamp_utc.astimezone(pytz.timezone(timezone_str)) 
            current_timestamp = max_timestamp[0]
            now = current_timestamp.astimezone(pytz.timezone(timezone_str))

            # Check if the timestamp is within the store's business hours
            for day, start, end in business_hours: 
                
                # Convert the store's start time and end time to a datetime object with timezone information
                start_time = datetime.combine(timestamp_local.date(), start, tzinfo=pytz.timezone('UTC')).astimezone(pytz.timezone(timezone_str))
                end_time = datetime.combine(timestamp_local.date(), end, tzinfo=pytz.timezone('UTC')).astimezone(pytz.timezone(timezone_str))
                
                # checks if the current timestamp_local falls within the store's business hours for a specific day
                if start_time <= timestamp_local <= end_time and timestamp_local.weekday() == day :
                    
                    # checking for hour
                    if now - timestamp_local <= timedelta(minutes=60):
                        if status.lower() == "active":
                            uptime_last_hour += 1
                        else:
                            downtime_last_hour += 1

                    # checking for day
                    if now - timestamp_local <= timedelta(days=1):
                        if status.lower() == "active":
                            uptime_last_day += 1
                        else:
                            downtime_last_day += 1

                    # checking for week
                    if now - timestamp_local <= timedelta(weeks=1):
                        if status.lower() == "active":
                            uptime_last_week += 1
                        else:
                            downtime_last_week += 1

        # Convert the uptime and downtime from minutes to hours for the last day and week
        uptime_last_day /= 60
        downtime_last_day /= 60
        uptime_last_week /= 60
        downtime_last_week /= 60

        # Write the results to the CSV file
        csv_writer.writerow({
            'store_id': store_id, 
            'uptime_last_hour': uptime_last_hour, 
            'uptime_last_day': uptime_last_day, 
            'uptime_last_week': uptime_last_week, 
            'downtime_last_hour': downtime_last_hour, 
            'downtime_last_day': downtime_last_day, 
            'downtime_last_week': downtime_last_week
        })
    

    except psycopg2.Error as e:
        print(f"Database error: {e}")
    except pytz.UnknownTimeZoneError as e:
        print(f"Timezone error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return(store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week)


@app.route('/trigger_report', methods=['POST'])
def trigger_report():

    # Generate a unique report_id
    global report_id
    report_status[report_id] = "Running"

    try:
        # Get all store_ids from the database
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT store_id FROM store_report_app_storehours;")
        store_ids = [row[0] for row in cur.fetchall()]
        cur.close()

        # Calculate uptime and downtime for all store_ids
        for store_id in store_ids:
            calculate_uptime_downtime(store_id, conn)

        # Once the report is complete, update the status
        report_status[report_id] = "Complete"
    except Exception as e:
        print(f"Unexpected error: {e}")
        report_status[report_id] = "Error"

    # Return the report_id as the output
    return jsonify({"report_id": report_id})

@app.route('/get_report', methods=['GET'])
def get_report():
    # Get the report_id from the request
    report_id = request.args.get('report_id')

    # Check if the report_id exists in the report_status dictionary
    if report_id not in report_status:
        return jsonify({"status": "Invalid report_id"}), 404

    # Check the status of the report
    status = report_status[report_id]

    # If the report is still running, return the status as "Running"
    if status == "Running":
        return jsonify({"status": "Running"})

    # If the report is complete, return the CSV file
    if status == "Complete":
        try:
            # Open the CSV file and read its contents
            with open('uptime_downtime.csv', 'r') as csvfile:
                csv_contents = csvfile.read()

            # Return the CSV contents as a response with appropriate headers
            return Response(
                csv_contents,
                headers={
                    "Content-Disposition": f"attachment;filename=uptime_downtime.csv",
                    "Content-Type": "text/csv"
                }
            )
        except Exception as e:
            print(f"Error while reading CSV file: {e}")
            return jsonify({"status": "Error"}), 500

if __name__ == '__main__':
    app.run(debug=True)