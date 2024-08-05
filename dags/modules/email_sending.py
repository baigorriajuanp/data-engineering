import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os

smtp_server = os.getenv('SMTP_HOST')
smtp_port = int(os.getenv('SMTP_PORT'))
sender_email = os.getenv('SMTP_USER')
password = os.getenv('MAIL_PASSWORD')

def send_email():
    subject = 'Datos cargados satisfactoriamente'
    body_text = 'Los datos fueron cargados a la base de datos exitosamente.'

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = os.getenv('MAIL_RECEIVER')
    msg['Subject'] = subject
    msg.attach(MIMEText(body_text, 'plain'))
    
    try:
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, password)
            server.sendmail(sender_email, sender_email, msg.as_string())
            print("Alerta enviada exitosamente.")
    except smtplib.SMTPAuthenticationError as e:
        print(f"Error de autenticación: {e.smtp_code} - {e.smtp_error}")
    except smtplib.SMTPException as e:
        print(f"Error al enviar la alerta: {e}")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")