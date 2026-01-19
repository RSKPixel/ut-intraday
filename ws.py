import asyncio
import websockets
import pandas as pd
import json
from tabulate import tabulate
import os

WS_URL = "ws://127.0.0.1:8080/ws"  # change to your endpoint


async def consume():
    websocket = None

    try:
        websocket = await websockets.connect(WS_URL)
        while True:
            json_response = await websocket.recv()
            response = json.loads(json_response)
            status = response.get("status", "")
            data = response.get("data", [])
            message = response.get("message", "")

            print("[WS]: ", message)

            if status == "ready":
                os.system("cls" if os.name == "nt" else "clear")
                print("[WS]: ", message)
            elif status == "success":
                status = "done"
                df = pd.DataFrame(data)
                df = df[
                    [
                        "datetime",
                        "symbol",
                        "tradingsymbol",
                        "signal",
                        "lot_size",
                        "signal_price",
                        "entry_price",
                        "stop_loss",
                    ]
                ]
                print(
                    tabulate(
                        df,
                        headers="keys",
                        tablefmt="psql",
                        colalign=(
                            "left",
                            "left",
                            "left",
                            "left",
                            "right",
                            "right",
                            "right",
                            "right",
                        ),
                        showindex=False,
                    )
                )

    except Exception as e:
        print(f"WebSocket connection error: {e}")
        if websocket:
            await websocket.close()
    finally:
        if websocket:
            await websocket.close()


if __name__ == "__main__":
    asyncio.run(consume())
