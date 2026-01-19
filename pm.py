from dependencies import kite_connect
import pandas as pd
import redis
import json
from tabulate import tabulate
import os

r = redis.Redis(host="localhost", port=6379, db=0)


def main():

    kite, status = kite_connect()

    if status["status"] != "success":
        print("Kite Connect Error:", status["message"])
        payload = {
            "status": "error",
            "data": [],
            "message": "Kite Connect Error: " + status["message"],
        }
        r.publish("pm", json.dumps(payload))
        return

    while True:
        os.system("cls" if os.name == "nt" else "clear")
        open_positions = kite.positions()
        open_orders = kite.orders()

        op = pd.DataFrame(open_positions["net"])
        op["name"] = op["tradingsymbol"].str[:-8]
        op = op[
            [
                "name",
                "tradingsymbol",
                "quantity",
                "average_price",
                "last_price",
                "m2m",
            ]
        ]

        oo = pd.DataFrame(open_orders)
        oo = oo[oo["status"] == "OPEN"]
        oo = oo[
            [
                "exchange",
                "tradingsymbol",
                "order_type",
                "transaction_type",
                "quantity",
                "price",
                "trigger_price",
                "pending_quantity",
            ]
        ]

        print("\nOpen Orders:")
        print(tabulate(oo, headers="keys", tablefmt="psql"))
        print("\nOpen Positions:")
        print(tabulate(op, headers="keys", tablefmt="psql"))

        cmd = input(":").lower()

        if cmd == "q":
            break
    pass


if __name__ == "__main__":
    main()
