import requests
from requests.adapters import HTTPAdapter, Retry
import resend
import unittest
import unittest.mock

resend.api_key = "re_EMsEH8fy_EkKMFhwyEwSA6QqTmAu3Q46K"

# Function to make requests


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
        r = resend.Emails.send({
            "from": "admin@gmail.com",
            "to":  "scarletmoon2003@gmail.com",
            "subject": "Request failed",
            "html": f"<p>Attempt {t} failed. Status code: {s.get(url).status_code. Error: {response_data['error']}}</p>"
        })

    return None

# Mock API test
