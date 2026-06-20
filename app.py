from flask import Flask, request, jsonify
import os
from datetime import datetime, time
import pytz
import requests


app = Flask(__name__)


# =========================
# SETTINGS
# =========================

MAX_CONTRACTS = 3
DEFAULT_CONTRACTS = 1

TZ = pytz.timezone("America/Chicago")

SESSION_START = time(8,30)
SESSION_END = time(15,0)


# =========================
# TRADESTATION
# =========================

TOKEN = os.getenv("TS_ACCESS_TOKEN")
ACCOUNT = os.getenv("TS_ACCOUNT")


BASE_URL = "https://api.tradestation.com/v3"



# =========================
# SESSION CHECK
# =========================

def market_open():

    now = datetime.now(TZ).time()

    return SESSION_START <= now <= SESSION_END



# =========================
# GET CURRENT POSITION
# =========================

def get_position(symbol):

    url = (
        BASE_URL +
        f"/brokerage/accounts/{ACCOUNT}/positions"
    )


    headers = {
        "Authorization":
        f"Bearer {TOKEN}"
    }


    r = requests.get(
        url,
        headers=headers
    )


    data = r.json()


    print(
        "POSITION CHECK:",
        data
    )


    qty = 0
    side = None



    for p in data.get("Positions", []):

        if p.get("Symbol") == symbol:

            qty = int(
                float(
                    p.get(
                    "Quantity",
                    0
                    )
                )
            )


            if qty > 0:
                side = "LONG"

            elif qty < 0:
                side = "SHORT"


    return abs(qty), side



# =========================
# SEND ORDER
# =========================

def send_order(
    symbol,
    action,
    qty
):

    url = (
        BASE_URL +
        "/orderexecution/orders"
    )


    headers = {

        "Authorization":
        f"Bearer {TOKEN}",

        "Content-Type":
        "application/json"
    }



    payload = {

        "AccountID": ACCOUNT,

        "Symbol": symbol,

        "Quantity": qty,

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
        url,
        headers=headers,
        json=payload
    )


    print(
        "TRADESTATION:",
        r.text
    )


    return r.json()



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
            {
            "error":
            "missing json"
            }
        ),400



    signal = data.get(
        "signal"
    )


    symbol = data.get(
        "symbol"
    )


    contracts = int(
        data.get(
            "contracts",
            DEFAULT_CONTRACTS
        )
    )


    if not symbol:

        return jsonify(
            {
            "error":
            "missing symbol"
            }
        ),400



    if contracts > MAX_CONTRACTS:

        contracts = MAX_CONTRACTS



    if not market_open():

        return jsonify(
            {
            "status":
            "ignored",
            "reason":
            "outside session"
            }
        )



    current_qty, current_side = get_position(symbol)



    result = None



    # =====================
    # LONG
    # =====================

    if signal == "LONG":


        # no position

        if current_qty == 0:


            result = send_order(
                symbol,
                "BUY",
                contracts
            )



        # already long = DCA

        elif current_side == "LONG":


            add = min(
                contracts,
                MAX_CONTRACTS-current_qty
            )


            if add <= 0:

                return jsonify(
                    {
                    "status":
                    "max contracts"
                    }
                )


            result = send_order(
                symbol,
                "BUY",
                add
            )



        else:


            return jsonify(
                {
                "status":
                "ignored",
                "reason":
                "currently short"
                }
            )



    # =====================
    # SHORT
    # =====================

    elif signal == "SHORT":


        if current_qty == 0:


            result = send_order(
                symbol,
                "SELLSHORT",
                contracts
            )



        elif current_side == "SHORT":


            add = min(
                contracts,
                MAX_CONTRACTS-current_qty
            )


            if add <= 0:

                return jsonify(
                    {
                    "status":
                    "max contracts"
                    }
                )


            result = send_order(
                symbol,
                "SELLSHORT",
                add
            )



        else:


            return jsonify(
                {
                "status":
                "ignored",
                "reason":
                "currently long"
                }
            )



    # =====================
    # EXIT
    # =====================

    elif signal == "EXIT":


        if current_qty == 0:

            return jsonify(
                {
                "status":
                "no position"
                }
            )



        if current_side == "LONG":


            result = send_order(
                symbol,
                "SELL",
                current_qty
            )



        elif current_side == "SHORT":


            result = send_order(
                symbol,
                "BUYTOCOVER",
                current_qty
            )



    else:


        return jsonify(
            {
            "error":
            "unknown signal"
            }
        ),400



    return jsonify(
        {
        "status":
        "sent",

        "signal":
        signal,

        "symbol":
        symbol,

        "contracts":
        contracts,

        "current_position":
        current_qty,

        "response":
        result
        }
    )



@app.route("/")
def home():

    return "MNQ TradeStation Bot Running"



if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=10000
    )