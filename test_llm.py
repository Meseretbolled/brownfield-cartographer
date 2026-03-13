import os
import requests
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("OPENROUTER_URL") + "/chat/completions"

headers = {
    "Authorization": f"Bearer {os.getenv('OPENROUTER_API_KEY')}",
    "Content-Type": "application/json",
}

payload = {
    "model": os.getenv("MODEL_NAME"),
    "messages": [
        {"role": "user", "content": "Explain what dbt models are."}
    ],
}

r = requests.post(url, headers=headers, json=payload)

print(r.json()["choices"][0]["message"]["content"])