import asyncio
import websockets
import pandas as pd
import json
from tabulate import tabulate

WS_URL = "ws://127.0.0.1:8080/ws"  # change to your endpoint


async def consume():
    async with websockets.connect(WS_URL) as websocket:
        print("Connected to WebSocket")

        state = "idle"  # states: idle -> expect_data -> expect_complete

        while True:
            msg = await websocket.recv()

            if state == "idle":
                if msg.lower() == "ready":
                    print("[WS] Ready received, expecting data next...")
                    state = "expect_data"
                else:
                    print("[WS] Received:", msg)

            elif state == "expect_data":
                # This MUST be the JSON payload
                try:
                    payload = json.loads(msg)
                    if isinstance(payload, list):
                        df = pd.DataFrame(payload)
                        print("[WS] DataFrame received:")
                        print(
                            tabulate(
                                df[
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
                                ],
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

                    else:
                        print("[WS] Non-list JSON payload:", payload)
                except Exception as e:
                    print("[WS] JSON decode error:", e, "raw:", msg)

                state = "expect_completed"
            elif state == "expect_completed":
                if msg.lower() == "completed":
                    print("[WS] Complete received. Cycle finished.")
                else:
                    print("[WS] Unexpected message during 'expect_completed':", msg)
                state = "idle"  # reset for next sequence


if __name__ == "__main__":
    asyncio.run(consume())
