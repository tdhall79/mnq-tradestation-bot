from flask import Flask, request, jsonify
import os
from datetime import datetime, time
import pytz
import requests


app = Flask(__name__)


# =========================
# SETTINGS
# =========================

TZ = pytz.timezone("America/Chicago")

SESSION_START = time(8,31)
SESSION_END = time(14,59)

MAX_CONTRACTS = 5


# =========================
# TRADESTATION SIM
# =========================

CLIENT_ID = os.getenv("TS_CLIENT_ID")
CLIENT_SECRET = os.getenv("TS_CLIENT_SECRET")

ACCESS_TOKEN = os.getenv("TS_ACCESS_TOKEN")

ACCOUNT = os.getenv("TS_ACCOUNT")


BASE_URL = "https://sim-api.tradestation.com/v3"



# =========================
# AUTH
# =========================

def auth_headers():

    return {
        "Authorization":
        f"Bearer {ACCESS_TOKEN}",

        "Content-Type":
        "application/json"
    }



# =========================
# SESSION
# =========================

def market_open():

    now = datetime.now(TZ).time()

    return SESSION_START <= now <= SESSION_END



# =========================
# POSITION
# =========================

def get_position(symbol):

    url = (
        BASE_URL +
        f"/brokerage/accounts/{ACCOUNT}/positions"
    )


    r = requests.get(
        url,
        headers=auth_headers()
    )


    data = r.json()


    print(
        "POSITION:",
        data
    )


    qty = 0
    side = None


    for p in data.get("Positions", []):

        if p.get("Symbol") == symbol:

            q = float(
                p.get("Quantity",0)
            )


            if q > 0:

                qty = int(q)
                side = "LONG"


            elif q < 0:

                qty = abs(int(q))
                side = "SHORT"


    return qty, side



# =========================
# ORDER
# =========================

def send_order(symbol, action, contracts):


    if contracts > MAX_CONTRACTS:

        contracts = MAX_CONTRACTS



    payload = {

        "AccountID": ACCOUNT,

        "Symbol": symbol,

        "Quantity": contracts,

        "OrderType": "Market",

        "TradeAction": action,

        "TimeInForce":
        {
            "Duration":"DAY"
        }
    }



    print(
        "ORDER:",
        payload
    )


    r = requests.post(

        BASE_URL +
        "/orderexecution/orders",

        headers=auth_headers(),

        json=payload
    )


    print(
        "TRADESTATION RESPONSE:",
        r.text
    )


    return r.json()



# =========================
# FLATTEN
# =========================

def flatten(symbol):

    qty, side = get_position(symbol)


    if qty == 0:

        return {
            "status":"already flat"
        }



    if side == "LONG":

        return send_order(
            symbol,
            "SELL",
            qty
        )


    if side == "SHORT":

        return send_order(
            symbol,
            "BUY",
            qty
        )



# =========================
# WEBHOOK
# =========================

@app.route("/webhook", methods=["POST"])
def webhook():


    data = request.json


    print(
        "WEBHOOK:",
        data
    )


    if not data:

        return jsonify(
            {"error":"missing json"}
        ),400



    symbol = data.get(
        "symbol"
    )


    signal = data.get(
        "signal"
    )


    contracts = int(
        data.get(
            "contracts",
            1
        )
    )


    if signal:

        signal = signal.upper().strip()



    if not symbol:

        return jsonify(
            {"error":"missing symbol"}
        ),400



    # outside EvoQ session

    if not market_open():

        result = flatten(symbol)

        return jsonify(
            {
            "status":"session closed",
            "flatten":result
            }
        )



    result = None



    # LONG + DCA LONG

    if signal == "LONG":

        result = send_order(
            symbol,
            "BUY",
            contracts
        )



    # SHORT + DCA SHORT

    elif signal == "SHORT":

        result = send_order(
            symbol,
            "SELL",
            contracts
        )



    # EXIT

    elif signal == "EXIT":

        result = flatten(symbol)



    else:

        return jsonify(
            {
            "error":"unknown signal",
            "signal":signal
            }
        ),400



    return jsonify(
        {
        "status":"sent",
        "symbol":symbol,
        "signal":signal,
        "contracts":contracts,
        "response":result
        }
    )



@app.route("/")
def home():

    return "TradeStation Futures SIM Bot Running"



if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=10000
    )