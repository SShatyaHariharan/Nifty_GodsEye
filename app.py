from flask import Flask, request, jsonify
from kiteconnect import KiteConnect, KiteTicker
from datetime import datetime, timedelta
import pandas as pd
import threading
import time

import gspread
from google.oauth2.service_account import Credentials

# ================= UTIL =================
def read_file(path):
    with open(path) as f:
        return f.read().strip()

# ================= CONFIG =================
API_KEY = read_file("config/api_key.txt")
API_SECRET = read_file("config/api_secret.txt")
ACCESS_TOKEN = read_file("config/access_token.txt")
TOKEN_FILE = "config/access_token.txt"

LOT_SIZE = 1
SL_PCT = 0.30
TARGET_PCT = 0.90
TRAIL_SL_PCT = 0.30
MAX_TRADE_DURATION_MIN = 15
CHECK_INTERVAL_SEC = 5

NIFTY_TOKEN = 256265

SPREADSHEET_NAME = "Kite Paper Trades"
SHEET_NAME = "Trades"

# =======================================

app = Flask(__name__)
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# ================= GOOGLE SHEETS =================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file(
    "config/google_creds.json", scopes=SCOPES
)

gc = gspread.authorize(creds)
sheet = gc.open(SPREADSHEET_NAME).worksheet(SHEET_NAME)

# ================= GLOBAL STATE =================
ltp_cache = {}
open_position = None
last_signal = None
lock = threading.Lock()

ws_connected = False
pending_subscriptions = set()

# ================= LOAD INSTRUMENTS =================
print("📥 Loading instruments...")
instruments = pd.DataFrame(kite.instruments("NFO"))
instruments = instruments[instruments["name"] == "NIFTY"]
instruments["expiry"] = pd.to_datetime(instruments["expiry"])
print("✅ Instruments loaded")

# ================= GOOGLE SHEET LOGGER =================
from datetime import datetime

def get_option_margin(symbol, price,lot_size):
    try:
        margin = kite.order_margins([{
            "exchange": "NFO",
            "tradingsymbol": symbol,
            "transaction_type": "BUY",
            "variety": "regular",
            "product": "MIS",
            "order_type": "MARKET",
            "quantity": lot_size,
            "price": price
        }])

        return margin[0]["total"]  # total margin required
    except Exception as e:
        print("⚠️ Margin fetch failed:", e)
        return None

def log_entry(row):
    sheet.append_row([
        row["entry_time"],
        "ENTRY",
        row["signal"],
        row["strike"],
        row["option"],
        row["entry_price"],
        round(row["margin"], 2),
        row["symbol"],
        "",                     # Reason
        0,                      # PnL ₹ (ENTRY)
        0                       # PnL %
    ], value_input_option="USER_ENTERED")

# def log_exit(row_idx, exit_price, pnl, reason):
#     """Update exit details in same row"""
#     sheet.update(f"B{row_idx}", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
#     sheet.update(f"C{row_idx}", reason)
#     sheet.update(f"I{row_idx}", round(exit_price, 2))
#     sheet.update(f"J{row_idx}", round(pnl, 2))
#     sheet.update(f"K{row_idx}", reason)

def log_exit(position, exit_price, pnl, pnl_pct, reason):
    sheet.append_row([
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "EXIT",
        position["signal"],
        position["strike"],
        position["option"],
        exit_price,
        round(position["margin"], 2),
        position["symbol"],
        reason,
        round(pnl, 2),
        round(pnl_pct, 2)
    ], value_input_option="USER_ENTERED")

# ================= WEBSOCKET =================
def on_ticks(ws, ticks):
    for t in ticks:
        ltp_cache[t["instrument_token"]] = t["last_price"]

def on_connect(ws, response):
    global ws_connected
    ws_connected = True
    ws.subscribe([NIFTY_TOKEN])
    ws.set_mode(ws.MODE_LTP, [NIFTY_TOKEN])

    if pending_subscriptions:
        ws.subscribe(list(pending_subscriptions))
        ws.set_mode(ws.MODE_LTP, list(pending_subscriptions))
        pending_subscriptions.clear()

def on_close(ws, code, reason):
    global ws_connected
    ws_connected = False
    print("WebSocket closed:", reason)

kws = KiteTicker(API_KEY, ACCESS_TOKEN)
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close
kws.connect(threaded=True)

# ================= SAFE SUBSCRIBE =================
def safe_subscribe(token):
    pending_subscriptions.add(token)
    if ws_connected and kws.ws:
        kws.subscribe(list(pending_subscriptions))
        kws.set_mode(kws.MODE_LTP, list(pending_subscriptions))
        pending_subscriptions.clear()

# ================= OPTION SELECTION =================
def get_nearest_option(nifty_ltp, option_type):
    atm = round(nifty_ltp / 50) * 50
    today = pd.Timestamp.now().normalize()

    df = instruments[
        (instruments["expiry"] >= today) &
        (instruments["strike"] == atm) &
        (instruments["instrument_type"] == option_type)
    ].sort_values("expiry")

    opt = df.iloc[0]
    return {
        "token": int(opt["instrument_token"]),
        "symbol": opt["tradingsymbol"],
        "strike": atm,
        "option": option_type,
        "lot_size": int(opt["lot_size"])
    }

# ================= EXIT HANDLER =================
def exit_trade(reason, exit_price):
    global open_position

    price_diff = exit_price - open_position["entry_price"]
    pnl = price_diff * LOT_SIZE

    pnl_pct = (pnl / open_position["margin"]) * 100

    log_exit(
    open_position,
    exit_price,
    pnl,
    pnl_pct,
    reason
    )

    print(f"🔴 EXIT {reason} | PnL: {pnl}")
    open_position = None

# ================= MONITOR THREAD =================
def monitor_position():
    global open_position

    while True:
        time.sleep(CHECK_INTERVAL_SEC)

        with lock:
            if not open_position:
                continue

            ltp = ltp_cache.get(open_position["token"])
            if not ltp:
                continue

            # Trailing SL
            new_sl = ltp * (1 - TRAIL_SL_PCT)
            if new_sl > open_position["sl"]:
                open_position["sl"] = new_sl

            if datetime.now() >= open_position["exit_time"]:
                exit_trade("TIME_EXIT", ltp)
                continue

            if ltp <= open_position["sl"]:
                exit_trade("SL_HIT", ltp)
            elif ltp >= open_position["target"]:
                exit_trade("TARGET_HIT", ltp)

threading.Thread(target=monitor_position, daemon=True).start()



# ================= accesstoken =================
@app.route("/accesstoken", methods=["GET"])
def generate_access_token():
    request_token = request.args.get("request_token")

    if not request_token:
        return "❌ request_token not found", 400

    try:
        data = kite.generate_session(
            request_token=request_token,
            api_secret=API_SECRET
        )

        access_token = data["access_token"]

        with open(TOKEN_FILE, "w") as f:
            f.write(access_token)

        return f"""
        <h2>✅ Access Token Generated</h2>
        <p><b>Time:</b> {datetime.now()}</p>
        <p>Token saved to <code>{TOKEN_FILE}</code></p>
        <p>You can close this tab.</p>
        """

    except Exception as e:
        return f"❌ Error generating token<br>{str(e)}", 500

# ================= WEBHOOK =================
@app.route("/webhook", methods=["POST"])
def webhook():
    global open_position, last_signal

    signal = request.json.get("signal")
    if signal not in ["BUY_CALL", "BUY_PUT"]:
        return jsonify({"error": "Invalid signal"})

    with lock:
        nifty_ltp = ltp_cache.get(NIFTY_TOKEN)
        if not nifty_ltp:
            return jsonify({"error": "NIFTY LTP not ready"})

        # Signal flip
        if open_position and signal != last_signal:
            ltp = ltp_cache.get(open_position["token"])
            if ltp:
                exit_trade("SIGNAL_FLIP", ltp)

        if open_position:
            return jsonify({"status": "Trade running"})

        option_type = "CE" if signal == "BUY_CALL" else "PE"
        opt = get_nearest_option(nifty_ltp, option_type)
        safe_subscribe(opt["token"])

        entry_price = None
        for _ in range(30):
            entry_price = ltp_cache.get(opt["token"])
            if entry_price:
                break
            time.sleep(0.1)

        if not entry_price:
            return jsonify({"error": "Option LTP not ready"})
        
        margin_price = get_option_margin(
            opt["symbol"],
            entry_price,
            opt["lot_size"]
        )

        if not margin_price:
            return jsonify({"error": "Margin not available"})

        sheet_row = log_entry({
            "entry_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "signal": signal,
            "strike": opt["strike"],
            "option": opt["option"],
            "symbol": opt["symbol"],
            "entry_price": round(entry_price, 2),
            "margin": margin_price
        })

        open_position = {
            "signal": signal,
            "token": opt["token"],
            "symbol": opt["symbol"],
            "strike": opt["strike"],
            "option": opt["option"],
            "entry_price": entry_price,
            "lot_size": opt["lot_size"],
            "margin": margin_price,
            "sl": entry_price * (1 - SL_PCT),
            "target": entry_price * (1 + TARGET_PCT),
            "exit_time": datetime.now() + timedelta(minutes=MAX_TRADE_DURATION_MIN)
        }

        last_signal = signal
        print(f"🟢 ENTRY {opt['symbol']} @ {entry_price}")
        return jsonify({"status": "ENTRY OK", "symbol": opt["symbol"]})

# ================= STATUS =================
@app.route("/status")
def status():
    return {
        "ws_connected": ws_connected,
        "nifty_ltp": ltp_cache.get(NIFTY_TOKEN),
        "open_trade": bool(open_position)
    }

# ================= MAIN =================
if __name__ == "__main__":
    print("🚀 Kite Paper Trading Engine Started")
    app.run(host="0.0.0.0", port=5000)
