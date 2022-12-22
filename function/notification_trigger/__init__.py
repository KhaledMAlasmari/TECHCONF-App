import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    # TODO: Get connection to database
    connection = psycopg2.connect(database="techconfdb", user="khaled1@techconfdbserver", password="Khaledxd1$", host="techconfdbserver.postgres.database.azure.com")
    cursor = connection.cursor()
    try:
        # TODO: Get notification message and subject from database using the notification_id
        cursor.execute("SELECT subject, message FROM notification WHERE id=%s;",[notification_id])
        notifications = cursor.fetchone()
        # TODO: Get attendees email and name
        cursor.execute("SELECT email, first_name, last_name FROM attendee;")
        attendees = cursor.fetchone()
        # TODO: Loop through each attendee and send an email with a personalized subject
        for attendee in attendees:
            #send a mail here!
            pass
        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        cursor.execute("UPDATE notification SET status=%s, completed_date=%s WHERE id=%s;", (f"Notified {len(attendees)} attendees", datetime.utcnow(), notification_id))
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        connection.close()
