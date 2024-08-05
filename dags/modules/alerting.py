import smtplib
from email.mime.text import MIMEText
import os

ALERT_THRESHOLD = 50000  #Ejemplo de umbral para alerta

def check_and_send_alert(data):
    for record in data:
        if 'current_price' not in record:
            raise ValueError(f"El diccionario no contiene la clave 'current_price': {record}")
        if record['current_price'] > ALERT_THRESHOLD:
            send_alert(
                f"Precio alto detectado: {record['name']}",
                f"El precio de {record['name']} ha sobrepasado el umbral de {ALERT_THRESHOLD}. Precio actual: {record['current_price']}"
            )

def send_alert(subject, body):
    smtp_host = os.getenv('SMTP_HOST')
    smtp_port = os.getenv('SMTP_PORT')
    smtp_user = os.getenv('SMTP_USER')
    smtp_password = os.getenv('MAIL_PASSWORD')

    from_email = smtp_user
    to_email = smtp_user

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = os.getenv('SMTP_USER')
    msg['To'] = os.getenv('MAIL_RECEIVER')

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(from_email, to_email, msg.as_string())
            print("Alerta enviada exitosamente.")
    except Exception as e:
        print(f"Error al enviar la alerta: {e}")