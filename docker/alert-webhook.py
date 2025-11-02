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
import os
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID_BOT", os.getenv("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID"))

def send_telegram_alert(alert_data):
    """Отправка алерта в Telegram с группировкой"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        
        alerts = alert_data.get('alerts', [])
        if not alerts:
            logger.warning("No alerts in webhook data")
            return
        
        # Фильтруем только firing алерты (resolved отключены в конфиге)
        firing_alerts = [a for a in alerts if a.get('status') == 'firing']
        
        # Отправляем только firing алерты
        if not firing_alerts:
            logger.info("No firing alerts to send (only resolved)")
            return
        
        # Формируем сообщение с группировкой
        if len(firing_alerts) == 1:
            # Один алерт - обычное сообщение
            alert = firing_alerts[0]
            alertname = alert.get('labels', {}).get('alertname', 'Unknown')
            severity = alert.get('labels', {}).get('severity', 'unknown')
            summary = alert.get('annotations', {}).get('summary', 'No summary')
            description = alert.get('annotations', {}).get('description', 'No description')
            
            emoji = "🚨" if severity == 'critical' else "⚠️"
            
            message = f"""
{emoji} *OKX Collector Alert*

*Alert:* {alertname}
*Severity:* {severity}
*Summary:* {summary}
*Description:* {description}
            """
        else:
            # Несколько алертов - группируем
            critical_count = sum(1 for a in firing_alerts if a.get('labels', {}).get('severity') == 'critical')
            warning_count = len(firing_alerts) - critical_count
            
            emoji = "🚨" if critical_count > 0 else "⚠️"
            
            message = f"""
{emoji} *OKX Collector Alerts* ({len(firing_alerts)} total)

*Critical:* {critical_count}
*Warning:* {warning_count}

*Alerts:*
"""
            for alert in firing_alerts[:5]:  # Показываем максимум 5
                alertname = alert.get('labels', {}).get('alertname', 'Unknown')
                severity = alert.get('labels', {}).get('severity', 'unknown')
                summary = alert.get('annotations', {}).get('summary', 'No summary')
                message += f"• {alertname} ({severity}): {summary}\n"
            
            if len(firing_alerts) > 5:
                message += f"... и еще {len(firing_alerts) - 5} алертов"
        
        data = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': message,
            'parse_mode': 'Markdown'
        }
        
        response = requests.post(url, data=data)
        if response.status_code == 200:
            logger.info(f"Telegram alert sent: {len(firing_alerts)} alert(s)")
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
