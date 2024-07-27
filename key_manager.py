import json
import os

import requests


class APIKeyManager:
    API_URL = "https://nline.io/api/public-data"
    TOKEN_FILE = ".access_token"

    def validate_or_retrieve_key(self):
        if self.verify_access_token():
            return "Valid API key found."
        else:
            return self.request_new_api_key()

    def verify_access_token(self):
        api_key = self.load_api_key()
        if not api_key:
            return False

        response = requests.post(
            self.API_URL, json={"action": "validate", "apiKey": api_key}
        )
        if response.status_code == 200:
            return response.json().get("valid", False)
        return False

    def request_new_api_key(self):
        print("No valid API key found. Requesting a new one...")
        name = input("Enter your name: ")
        organization = input("Enter your organization: ")
        email = input("Enter your email: ")
        intent = input("Enter your intended use of the data: ")

        api_key = self.request_api_key(name, organization, email, intent)
        self.save_api_key(api_key)
        return "API key received and saved."

    def request_api_key(self, name, organization, email, intent):
        data = {
            "action": "request",
            "name": name,
            "organization": organization,
            "email": email,
            "intent": intent,
        }
        response = requests.post(self.API_URL, json=data)
        if response.status_code == 200:
            return response.json()["apiKey"]
        else:
            raise Exception(f"Failed to get API key: {response.text}")

    def save_api_key(self, api_key):
        with open(self.TOKEN_FILE, "w") as f:
            json.dump({"token": api_key}, f)

    def load_api_key(self):
        if os.path.exists(self.TOKEN_FILE):
            with open(self.TOKEN_FILE, "r") as f:
                return json.load(f)["token"]
        return None
