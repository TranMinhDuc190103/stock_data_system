import requests
from requests.adapters import HTTPAdapter, Retry
import resend

resend.api_key = "re_EMsEH8fy_EkKMFhwyEwSA6QqTmAu3Q46K"

# Function to make requests

def send_email(from_email, to_email, subject, html):
    resend.Emails.send({
        "from": from_email,
        "to": to_email,
        "subject": subject,
        "html": html
    })

def make_requests(url, max_retries=5, success_list=[200]):
    s = requests.Session()
    for t in range(max_retries):
        try:
            response = s.get(url)
            if response.status_code == 200:
                return response
        except requests.exceptions.ConnectionError:
            print(f"Attempt {t} failed. Try again.")
            
    response = s.get(url)
    if response.status_code not in success_list:
        response_data = response.json()
        send_email(
            from_email="admin@gmail.com",
            to_email="scarletmoon2003@gmail.com",
            subject="DAG failed",
            html=response_data
        )
    return None

def failed_email():
    send_email(
        "admin@gmail.com",
        "scarletmoon2003@gmail.com",
        "DAG failed",
        "<p>DAG failed<p>"
    )
    
def retry_request():
    send_email(
        "admin@gmail.com",
        "scarletmoon2003@gmail.com",
        "Permission to retry",
        "<p>DAG failed. Request for permission to retry DAG again.</p>"
    )
    
def callback_on_success(context):
    r = resend.Emails.send({
        "admin@gmail.com",
        "scarletmoon2003@gmail.com",
        "DAG run complete",
        "<p> DAG run complete. </p>"
    })
    
def callback_on_failure(context):
    failed_email()
# Mock API test
