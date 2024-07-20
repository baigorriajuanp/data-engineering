import smtplib
from email.mime.text import MIMEText

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
    smtp_host = 'smtp.example.com'
    smtp_port = 587
    smtp_user = 'tu_usuario@example.com'
    smtp_password = 'tu_contraseña'

    from_email = 'alertas@example.com'
    to_email = 'destinatario@example.com'

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(from_email, to_email, msg.as_string())
            print("Alerta enviada exitosamente.")
    except Exception as e:
        print(f"Error al enviar la alerta: {e}")