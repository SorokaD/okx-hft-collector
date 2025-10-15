#!/usr/bin/env python3
"""
Simple webhook server for receiving Prometheus alerts
"""
import json
import logging
import requests
from flask import Flask, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Настройки для отправки уведомлений
TELEGRAM_BOT_TOKEN = "YOUR_BOT_TOKEN"  # Замените на ваш токен
TELEGRAM_CHAT_ID = "YOUR_CHAT_ID"     # Замените на ваш chat ID

def send_telegram_alert(alert_data):
    """Отправка алерта в Telegram"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        
        # Формируем сообщение
        alert = alert_data.get('alerts', [{}])[0]
        status = alert.get('status', 'unknown')
        alertname = alert.get('labels', {}).get('alertname', 'Unknown')
        summary = alert.get('annotations', {}).get('summary', 'No summary')
        description = alert.get('annotations', {}).get('description', 'No description')
        
        if status == 'firing':
            emoji = "🚨"
        else:
            emoji = "✅"
            
        message = f"""
{emoji} *OKX Collector Alert*

*Alert:* {alertname}
*Status:* {status}
*Summary:* {summary}
*Description:* {description}
        """
        
        data = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': message,
            'parse_mode': 'Markdown'
        }
        
        response = requests.post(url, data=data)
        if response.status_code == 200:
            logger.info(f"Telegram alert sent: {alertname}")
        else:
            logger.error(f"Failed to send Telegram alert: {response.text}")
            
    except Exception as e:
        logger.error(f"Error sending Telegram alert: {e}")

def send_email_alert(alert_data):
    """Отправка алерта по email (заглушка)"""
    logger.info(f"Email alert: {json.dumps(alert_data, indent=2)}")

@app.route('/webhook', methods=['POST'])
def webhook():
    """Обработка webhook от Alertmanager"""
    try:
        alert_data = request.get_json()
        logger.info(f"Received alert: {json.dumps(alert_data, indent=2)}")
        
        # Отправляем в Telegram
        send_telegram_alert(alert_data)
        
        # Отправляем по email
        send_email_alert(alert_data)
        
        return {"status": "success"}, 200
        
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        return {"status": "error", "message": str(e)}, 500

@app.route('/health')
def health():
    """Health check endpoint"""
    return {"status": "healthy"}, 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=False)
