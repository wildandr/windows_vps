import requests
import json

def send_to_telegram(json_input):
    # Parse the JSON input
    data = json.loads(json_input)
    message = data.get('message')
    
    # Your Telegram bot token and chat ID
    bot_token = '7543275222:AAEnLj4orL7Poz0VQR_gC1eDVmp0mJT4FTE'
    chat_id = '1736931166'
    
    # Telegram API URL
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    
    # Payload to send to Telegram
    payload = {
        'chat_id': chat_id,
        'text': message
    }
    
    # Send the request
    response = requests.post(url, json=payload)
    
    # Check the response
    if response.status_code == 200:
        print('Message sent successfully')
    else:
        print(f'Failed to send message: {response.status_code}')