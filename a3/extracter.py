from ib_insync import *
import time
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer
import json

print("Import statements executed.")

# Connect to IB
ib = IB()
print("IB instance created.")
ib.connect('127.0.0.1', 7497, clientId=1)  # Adjust with your TWS port
print("Connected to IB.")

# Available contracts
available_contracts = {
    'EURUSD': Forex('EURUSD'),
    'GBPUSD': Forex('GBPUSD'),
    'USDJPY': Forex('USDJPY'),
    # Add more symbols here
}
print("Available contracts initialized.")

# Kafka producer configuration
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',  # Adjust with your Kafka broker details
    'client.id': 'forex-data-producer'
}
print("Kafka configuration set.")
producer = Producer(kafka_conf)
print("Kafka producer initialized.")

def get_prev_month_close(contract):
    print(f"Fetching previous month close for {contract.symbol}")
    today_utc = datetime.now(timezone.utc)
    first_day_of_current_month = today_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_month_end = first_day_of_current_month - timedelta(seconds=1)
    end_date_time = last_month_end.strftime('%Y%m%d-%H:%M:%S')
    print(f"End date for historical data: {end_date_time}")
    prev_month_data = ib.reqHistoricalData(
        contract,
        endDateTime=end_date_time,
        durationStr='1 M',
        barSizeSetting='1 day',
        whatToShow='MIDPOINT',
        useRTH=True,
        formatDate=1
    )
    print("Historical data retrieved.")
    return prev_month_data[-1].close if prev_month_data else None

def get_vwap(contract):
    print(f"Calculating VWAP for {contract.symbol}")
    bars = ib.reqHistoricalData(
        contract,
        endDateTime='',
        durationStr='5 D',
        barSizeSetting='1 day',
        whatToShow='MIDPOINT',
        useRTH=True,
        formatDate=1
    )
    print("Historical data for VWAP retrieved.")
    if not bars:
        print("No bars found.")
        return None
    total_volume = sum(bar.volume for bar in bars)
    total_vwap = sum(bar.close * bar.volume for bar in bars)
    if total_volume == 0:
        print("Total volume is zero, unable to calculate VWAP.")
        return None
    vwap = total_vwap / total_volume
    print(f"Calculated VWAP: {vwap}")
    return vwap

def send_data_to_kafka(data):
    print("Sending data to Kafka.")
    try:
        producer.produce('forex-data', json.dumps(data).encode('utf-8'), callback=delivery_report)
        producer.flush()
        print("Data sent successfully.")
    except Exception as e:
        print("Error sending data to Kafka:", str(e))

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def start_data_extraction(contracts):
    print("Starting data extraction.")
    prev_month_closes = {contract.symbol: get_prev_month_close(contract) for contract in contracts}
    print("Previous month closes retrieved.")
    try:
        while True:
            print("Collecting data for all contracts.")
            for contract in contracts:
                print(f"Fetching data for {contract.symbol}")
                bars = ib.reqHistoricalData(
                    contract,
                    endDateTime='',
                    durationStr='1 D',
                    barSizeSetting='1 day',
                    whatToShow='MIDPOINT',
                    useRTH=True,
                    formatDate=1
                )
                print(f"Data retrieved for {contract.symbol}")
                if bars:
                    for bar in bars:
                        open_pct = ((bar.close - bar.open) / bar.open) * 100
                        high_pct = ((bar.close - bar.high) / bar.high) * 100
                        low_pct = ((bar.close - bar.low) / bar.low) * 100
                        prev_close_pct = ((prev_month_closes[contract.symbol] - bar.close) / prev_month_closes[contract.symbol]) * 100
                        vwap = get_vwap(contract)
                        vwap_pct = ((bar.close - vwap) / vwap) * 100 if vwap else None

                        mid = (bar.high + bar.low) / 2
                        uhigh = (mid + bar.high) / 2
                        llow = (mid + bar.low) / 2
                        uuh = (uhigh + bar.high) / 2
                        lll = (llow + bar.low) / 2

                        timestamp = bar.date.strftime('%Y-%m-%d %H:%M:%S')
                        contract_data = {
                            'symbol': contract.symbol,
                            'timestamp': bar.date.strftime('%Y-%m-%d %H:%M:%S'),
                            'open': bar.open,
                            'high': bar.high,
                            'low': bar.low,
                            'close': bar.close,
                            'open_pct_change': round(open_pct, 2),
                            'high_pct_change': round(high_pct, 2),
                            'low_pct_change': round(low_pct, 2),
                            'prev_close_pct_change': round(prev_close_pct, 2),
                            'vwap': vwap,
                            'vwap_pct_change': round(vwap_pct, 2) if vwap_pct else None,
                            'mid': mid,
                            'uhigh': uhigh,
                            'llow': llow,
                            'uuh': uuh,
                            'lll': lll
                        }
                        send_data_to_kafka(contract_data)
                        print(contract_data)
            time.sleep(5)
    except KeyboardInterrupt:
        print("Stopping data collection.")
    finally:
        ib.disconnect()
        print("Disconnected from IB.")

if __name__ == "__main__":
    selected_symbols = ['EURUSD', 'GBPUSD', 'USDJPY']
    selected_contracts = [available_contracts[symbol] for symbol in selected_symbols]
    print(f"Starting data extraction for: {', '.join(selected_symbols)}")
    start_data_extraction(selected_contracts)
