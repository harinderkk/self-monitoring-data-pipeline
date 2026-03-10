import requests
from bs4 import BeautifulSoup

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

url = "https://www.sedarplus.ca/landingpage/"

response = requests.get(url, headers=headers)

print(f"Status code: {response.status_code}")
print(f"Page title: {BeautifulSoup(response.text, 'html.parser').title.text if response.status_code == 200 else 'Could not fetch'}")