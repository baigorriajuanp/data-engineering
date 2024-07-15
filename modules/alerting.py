import smtplib
from email.mime.text import MIMEText

ALERT_THRESHOLD = 50000  # Ejemplo de umbral para alerta

def check_and_send_alert(data):
    for record in data:
        if record['current_price'] > ALERT_THRESHOLD:
            send_alert(f"Precio alto detectado: {record['name']}", f"El precio de {record['name']} ha sobrepasado el umbral de {ALERT_THRESHOLD}. Precio actual: {record['current_price']}")

def send_alert(subject, body):
    to_email = 'alert@example.com'
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = 'your_email@example.com'
    msg['To'] = to_email

    with smtplib.SMTP('smtp.example.com') as server:
        server.login('your_email@example.com', 'password')
        server.sendmail('your_email@example.com', to_email, msg.as_string())