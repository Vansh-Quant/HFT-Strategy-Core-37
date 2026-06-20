import asyncio
import json
import websockets

class DataIngressLayer:
    def __init__(self, stream_url: str, data_queue: asyncio.Queue):
        self.stream_url = stream_url
        self.data_queue = data_queue
        self.is_running = False

    async def stream_receiver_loop(self):
        """
        Maintains connection to exchange WebSocket stream and feeds the shared system queue.
        """
        self.is_running = True
        
        while self.is_running:
            try:
                async with websockets.connect(self.stream_url, open_timeout=15) as ws:
                    async for raw_msg in ws:
                        # Decode payload and immediately forward to async queue
                        payload = json.loads(raw_msg)
                        await self.data_queue.put(payload)
                        
            except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError):
                # Automatic silent reconnection sequence matching our recovery rule
                await asyncio.sleep(3)
            except Exception:
                await asyncio.sleep(3)

    def stop_stream(self):
        self.is_running = False