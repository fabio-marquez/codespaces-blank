import base64
import os

import requests


class SpotifyAPI:
    def __init__(self):
        self.client_id = os.environ.get('SPOTIFY_CLIENT_ID')
        self.client_secret = os.environ.get('SPOTIFY_SECRET')

    def __auth(self) -> str:
        auth_url = 'https://accounts.spotify.com/api/token'
        auth_header = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        headers = {'Authorization': f'Basic {auth_header}'}
        data = {'grant_type': 'client_credentials'}
        response = requests.post(auth_url, headers=headers, data=data)
        response_data = response.json()
        return response_data['access_token']

    def get_top50_releases(self) -> dict:
        token = self.__auth()
        api_url = f'https://api.spotify.com/v1/browse/new-releases'
        headers = {'Authorization': f'Bearer {token}'}
        params = {'limit': 50, 'offset': '0'}
        response = requests.get(api_url, headers=headers, params=params)
        return response.json()