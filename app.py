from flask import Flask, request, jsonify
from kiteconnect import KiteConnect, KiteTicker
from datetime import datetime, timedelta
import pandas as pd
import threading
import time
import json

import gspread
from google.oauth2.service_account import Credentials

# ================= UTIL =================
def read_file(path):
    try:
        with open(path) as f:
            return f.read().strip()
    except FileNotFoundError:
        return None

def write_file(path, content):
    with open(path, "w") as f:
        f.write(content)

# ================= CONFIG =================
API_KEY = read_file("config/api_key.txt")
API_SECRET = read_file("config/api_secret.txt")
ACCESS_TOKEN_FILE = "config/access_token.txt"

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

# ================= DYNAMIC KITE MANAGER =================
class KiteManager:
    def __init__(self):
        self.api_key = API_KEY
        self.api_secret = API_SECRET
        self.access_token = read_file(ACCESS_TOKEN_FILE)
        self.kite = None
        self.kws = None
        self.ws_connected = False
        self.pending_subscriptions = set()
        self.lock = threading.Lock()
        self.initialize()
    
    def initialize(self):
        """Initialize or reinitialize Kite connections"""
        with self.lock:
            # Close existing WebSocket if any
            if self.kws:
                try:
                    self.kws.close()
                    time.sleep(1)  # Give time to close gracefully
                except:
                    pass
            
            if not self.access_token:
                print("⚠️ No access token available. Please generate one via /accesstoken")
                return False
            
            try:
                # Initialize KiteConnect
                self.kite = KiteConnect(api_key=self.api_key)
                self.kite.set_access_token(self.access_token)
                
                # Test the token validity
                profile = self.kite.profile()
                print(f"✅ Kite Connected: {profile.get('user_name', 'Unknown')}")
                
                # Initialize WebSocket
                self.kws = KiteTicker(self.api_key, self.access_token)
                self.kws.on_ticks = self.on_ticks
                self.kws.on_connect = self.on_connect
                self.kws.on_close = self.on_close
                self.kws.on_error = self.on_error
                self.kws.on_reconnect = self.on_reconnect
                self.kws.on_noreconnect = self.on_noreconnect
                
                # Start WebSocket in thread
                self.kws.connect(threaded=True)
                
                return True
                
            except Exception as e:
                print(f"❌ Failed to initialize Kite: {e}")
                self.kite = None
                self.kws = None
                return False
    
    def update_access_token(self, new_token):
        """Update access token and reinitialize connections"""
        self.access_token = new_token
        write_file(ACCESS_TOKEN_FILE, new_token)
        success = self.initialize()
        if success:
            # Resubscribe to instruments if position is open
            global open_position
            if open_position:
                self.subscribe(open_position["token"])
            # Always subscribe to NIFTY
            self.subscribe(NIFTY_TOKEN)
        return success
    
    def on_ticks(self, ws, ticks):
        for t in ticks:
            ltp_cache[t["instrument_token"]] = t["last_price"]
    
    def on_connect(self, ws, response):
        self.ws_connected = True
        print("🟢 WebSocket Connected")
        ws.subscribe([NIFTY_TOKEN])
        ws.set_mode(ws.MODE_LTP, [NIFTY_TOKEN])
        
        # Handle pending subscriptions
        if self.pending_subscriptions:
            ws.subscribe(list(self.pending_subscriptions))
            ws.set_mode(ws.MODE_LTP, list(self.pending_subscriptions))
            self.pending_subscriptions.clear()
    
    def on_close(self, ws, code, reason):
        self.ws_connected = False
        print(f"🔴 WebSocket Closed: {reason}")
    
    def on_error(self, ws, code, reason):
        print(f"⚠️ WebSocket Error: {code} - {reason}")
    
    def on_reconnect(self, ws, attempt_count):
        print(f"🔄 WebSocket Reconnecting... Attempt {attempt_count}")
    
    def on_noreconnect(self, ws):
        print("❌ WebSocket Max Reconnects Reached")
        self.ws_connected = False
    
    def subscribe(self, token):
        """Thread-safe subscribe"""
        if not token:
            return
        
        if self.ws_connected and self.kws and self.kws.ws:
            try:
                self.kws.subscribe([token])
                self.kws.set_mode(self.kws.MODE_LTP, [token])
            except Exception as e:
                print(f"Subscribe error: {e}")
        else:
            self.pending_subscriptions.add(token)
    
    def get_kite(self):
        """Get current kite instance"""
        return self.kite
    
    def get_kws(self):
        """Get current websocket instance"""
        return self.kws
    
    def is_ready(self):
        """Check if kite is initialized"""
        return self.kite is not None


# Global Kite Manager instance
kite_manager = KiteManager()


# ================= LOAD INSTRUMENTS =================
def load_instruments():
    """Load instruments with retry logic"""
    global instruments
    while True:
        try:
            if not kite_manager.is_ready():
                print("⏳ Waiting for Kite connection to load instruments...")
                time.sleep(5)
                continue
                
            print("📥 Loading instruments...")
            instruments = pd.DataFrame(kite_manager.get_kite().instruments("NFO"))
            instruments = instruments[instruments["name"] == "NIFTY"]
            instruments["expiry"] = pd.to_datetime(instruments["expiry"])
            print("✅ Instruments loaded")
            break
        except Exception as e:
            print(f"⚠️ Failed to load instruments: {e}. Retrying in 5s...")
            time.sleep(5)

# Start instrument loading in background
instruments = None
threading.Thread(target=load_instruments, daemon=True).start()

# ================= GOOGLE SHEET LOGGER =================
def get_option_margin(symbol, price, lot_size):
    try:
        kite = kite_manager.get_kite()
        if not kite:
            return None
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
        return margin[0]["total"]
    except Exception as e:
        print("⚠️ Margin fetch failed:", e)
        return None

def log_entry(position_data):
    """Log entry and return the row number for later exit update"""
    row = [
        position_data["entry_time"],  # A: Entry Time
        "",                           # B: Exit Time (empty initially)
        position_data["signal"],      # C: Signal
        position_data["strike"],      # D: Strike
        position_data["option"],      # E: Option Type
        position_data["entry_price"], # F: Entry LTP
        "",                           # G: Exit LTP (empty initially)
        round(position_data["margin"], 2),  # H: Entry Margin
        "",                           # I: Exit Margin (empty initially)
        "",                           # J: P/L (empty initially)
        "",                           # K: P/L % (empty initially)
        position_data["symbol"],      # L: Symbol
        ""                            # M: Exit Reason (empty initially)
    ]
    sheet.append_row(row, value_input_option="USER_ENTERED")
    return len(sheet.get_all_values())

def log_exit(row_idx, position, exit_price, exit_margin, pnl, pnl_pct, reason):
    """Update the existing row with exit details"""
    sheet.update_cell(row_idx, 2, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))  # B: Exit Time
    sheet.update_cell(row_idx, 7, round(exit_price, 2))   # G: Exit LTP
    sheet.update_cell(row_idx, 9, round(exit_margin, 2))  # I: Exit Margin
    sheet.update_cell(row_idx, 10, round(pnl, 2))         # J: P/L
    sheet.update_cell(row_idx, 11, round(pnl_pct, 2))     # K: P/L %
    sheet.update_cell(row_idx, 13, reason)                # M: Exit Reason

# ================= SAFE SUBSCRIBE =================
def safe_subscribe(token):
    kite_manager.subscribe(token)

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

    if not open_position:
        return

    entry_margin = open_position["margin"]
    exit_margin = get_option_margin(open_position["symbol"], exit_price, open_position["lot_size"])
    
    if not exit_margin:
        exit_margin = entry_margin

    price_diff = exit_price - open_position["entry_price"]
    pnl = price_diff * open_position["lot_size"]
    pnl_pct = (pnl / entry_margin) * 100 if entry_margin else 0

    log_exit(
        open_position["sheet_row"],
        open_position,
        exit_price,
        exit_margin,
        pnl,
        pnl_pct,
        reason
    )

    print(f"🔴 EXIT {reason} | {open_position['symbol']} | PnL: ₹{pnl:.2f} ({pnl_pct:.2f}%)")
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

# ================= ACCESS TOKEN =================
@app.route("/accesstoken", methods=["GET"])
def generate_access_token():
    request_token = request.args.get("request_token")

    if not request_token:
        # If no request token, show status page
        current_token = read_file(ACCESS_TOKEN_FILE)
        status = "✅ Active" if kite_manager.is_ready() else "❌ Not Connected"
        
        return f"""
        <html>
        <head><title>Kite Token Manager</title></head>
        <body>
            <h2>Kite Access Token Manager</h2>
            <p><b>Status:</b> {status}</p>
            <p><b>Current Token:</b> {'*' * 10}{current_token[-5:] if current_token else 'None'}</p>
            <hr>
            <h3>Generate New Token</h3>
            <form method="get">
                <input type="text" name="request_token" placeholder="Enter Request Token" size="50">
                <button type="submit">Generate & Update</button>
            </form>
            <p><i>Service will auto-reload without restart</i></p>
        </body>
        </html>
        """, 200

    try:
        # Generate new session
        kite = KiteConnect(api_key=API_KEY)
        data = kite.generate_session(
            request_token=request_token,
            api_secret=API_SECRET
        )
        
        new_access_token = data["access_token"]
        
        # Update kite manager with new token (hot reload)
        success = kite_manager.update_access_token(new_access_token)
        
        if success:
            return f"""
            <html>
            <head><title>Success</title></head>
            <body>
                <h2>✅ Access Token Updated Successfully</h2>
                <p><b>Time:</b> {datetime.now()}</p>
                <p>Token saved and service reconnected without restart!</p>
                <p>WebSocket: {'Connected' if kite_manager.ws_connected else 'Connecting...'}</p>
                <br>
                <a href="/accesstoken">Check Status</a> | 
                <a href="/status">System Status</a>
            </body>
            </html>
            """
        else:
            return f"""
            <html>
            <head><title>Error</title></head>
            <body>
                <h2>⚠️ Token Generated but Connection Failed</h2>
                <p>Token saved to file, but failed to initialize connection.</p>
                <p>Error: Check logs</p>
                <br>
                <a href="/accesstoken">Try Again</a>
            </body>
            </html>
            """, 500

    except Exception as e:
        return f"""
        <html>
        <head><title>Error</title></head>
        <body>
            <h2>❌ Error Generating Token</h2>
            <p>{str(e)}</p>
            <br>
            <a href="/accesstoken">Back</a>
        </body>
        </html>
        """, 500

@app.route("/refreshtoken", methods=["POST"])
def refresh_token_api():
    """API endpoint to refresh token programmatically"""
    try:
        data = request.json
        request_token = data.get("request_token")
        
        if not request_token:
            return jsonify({"error": "request_token required"}), 400
        
        kite = KiteConnect(api_key=API_KEY)
        session = kite.generate_session(
            request_token=request_token,
            api_secret=API_SECRET
        )
        
        new_token = session["access_token"]
        success = kite_manager.update_access_token(new_token)
        
        return jsonify({
            "success": success,
            "message": "Token updated and reconnected" if success else "Token saved but connection failed",
            "websocket_connected": kite_manager.ws_connected
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ================= WEBHOOK =================
@app.route("/webhook", methods=["POST"])
def webhook():
    global open_position, last_signal

    if not kite_manager.is_ready():
        return jsonify({"error": "Kite not connected. Please generate access token."}), 503

    signal = request.json.get("signal")
    if signal not in ["BUY_CALL", "BUY_PUT"]:
        return jsonify({"error": "Invalid signal"}), 400

    with lock:
        nifty_ltp = ltp_cache.get(NIFTY_TOKEN)
        if not nifty_ltp:
            return jsonify({"error": "NIFTY LTP not ready"}), 503

        # Signal flip
        if open_position and signal != last_signal:
            ltp = ltp_cache.get(open_position["token"])
            if ltp:
                exit_trade("SIGNAL_FLIP", ltp)

        if open_position:
            return jsonify({"status": "Trade running"}), 200

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
            return jsonify({"error": "Option LTP not ready"}), 503
        
        margin_price = get_option_margin(
            opt["symbol"],
            entry_price,
            opt["lot_size"]
        )

        if not margin_price:
            return jsonify({"error": "Margin not available"}), 503

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
            "exit_time": datetime.now() + timedelta(minutes=MAX_TRADE_DURATION_MIN),
            "sheet_row": sheet_row
        }

        last_signal = signal
        print(f"🟢 ENTRY {opt['symbol']} @ {entry_price} | Row: {sheet_row}")
        return jsonify({"status": "ENTRY OK", "symbol": opt["symbol"], "row": sheet_row}), 200

# ================= STATUS =================
@app.route("/status")
def status():
    return jsonify({
        "kite_connected": kite_manager.is_ready(),
        "websocket_connected": kite_manager.ws_connected,
        "nifty_ltp": ltp_cache.get(NIFTY_TOKEN),
        "open_trade": bool(open_position),
        "position": open_position if open_position else None,
        "instruments_loaded": instruments is not None
    })

@app.route("/health")
def health():
    """Simple health check"""
    return jsonify({
        "status": "healthy",
        "kite_ready": kite_manager.is_ready(),
        "ws_connected": kite_manager.ws_connected
    })

# ================= MAIN =================
if __name__ == "__main__":
    print("🚀 Kite Paper Trading Engine Started")
    print(f"📡 API Key: {API_KEY[:5]}...{API_KEY[-5:] if len(API_KEY) > 10 else ''}")
    print(f"🔑 Access Token: {'Loaded' if kite_manager.access_token else 'Not Found'}")
    print("📝 Visit /accesstoken to generate or refresh token")
    app.run(host="0.0.0.0", port=5000, debug=False)
