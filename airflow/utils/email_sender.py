# utils/email_sender.py
# Email sending logic (SMTP or 3rd party)

import smtplib # Simple Mail Transfer Protocol library. Used to send emails via an SMTP server (e.g., Gmail, Outlook, etc.).

from pathlib import Path 
from email.message import EmailMessage # class used to construct and structure an email.
                                       # Supports setting subject, sender, recipient, body, and adding attachments.
from email.utils import formataddr # Helps format the "From" and "To" fields of an email with a name + email format. 
from utils.logger import setup_logger

# put this to reuse the same logger in any script
logger = setup_logger()


def send_email_with_attachment(email_sender, email_password, to_email, subject, body, attachment_paths):
    """
    Send an email with one or more attachments.

    Parameters:
    - email_sender; str, sender email address
    - email_password: str, sender email password
    - to_email: str, recipient email address
    - subject: str, email subject
    - body: str, email body (plain text)
    - attachment_paths: list of str or Path, file paths to attach
    """
    msg = EmailMessage()
    msg["From"] = formataddr(("JCDE Bot", email_sender))
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    # Handle attachments
    if isinstance(attachment_paths, (str, Path)):
        attachment_paths = [attachment_paths]

    for file_path in attachment_paths:
        file_path = Path(file_path)
        if file_path.exists():
            with open(file_path, "rb") as f:
                file_data = f.read()
                file_name = file_path.name
                msg.add_attachment(
                    file_data,
                    maintype="application",
                    subtype="octet-stream",
                    filename=file_name,
                )
        else:
            logger.warning(f"⚠️ Attachment file not found: {file_path}")

    # Send email
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(email_sender, email_password)
            smtp.send_message(msg)
            logger.info(f"✅ Email sent successfully.")
        
    except Exception as e:
        logger.error(f"❌ Failed to send email: {e}")

