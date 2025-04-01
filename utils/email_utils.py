import smtplib
from email.message import EmailMessage
import os
from dotenv import load_dotenv

load_dotenv()

OUTLOOK_EMAIL = os.getenv("OUTLOOK_EMAIL")
OUTLOOK_PASSWORD = os.getenv("OUTLOOK_PASSWORD")

def send_email_with_json(recipients, subject, json_content, filename):
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = OUTLOOK_EMAIL
    msg['To'] = ", ".join(recipients)
    msg.set_content("ETL workflow completed successfully. Please find the summary JSON attached.")

    msg.add_attachment(json_content, maintype='application', subtype='json', filename=filename)

    with smtplib.SMTP('smtp.office365.com', 587) as smtp:
        smtp.starttls()
        smtp.login(OUTLOOK_EMAIL, OUTLOOK_PASSWORD)
        smtp.send_message(msg)
