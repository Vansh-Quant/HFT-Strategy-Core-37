import asyncio
import websockets
import json
import time
import numpy as np

STREAM_URL = "wss://stream.binance.com:9443/ws/btcusdt@depth5@100ms"

class RealTimeRiskEngine:
    """Institutional-grade risk layer tracking state vectors before order routing."""
    def __init__(self, max_position_cap=3, max_drawdown_limit=-50.0):
        self.max_position_cap = max_position_cap
        self.max_drawdown_limit = max_drawdown_limit
        self.current_position = 0
        self.cumulative_pnl = 0.0

    def check_risk_clearance(self, prospective_order_side):
        """Evaluates systematic risk boundaries. Returns True if order is safe to route."""
        # Check Position Cap constraints
        if prospective_order_side == "BUY" and self.current_position >= self.max_position_cap:
            return False, "REJECTED: Max Long Position Limit Breached"
        if prospective_order_side == "SELL" and self.current_position <= -self.max_position_cap:
            return False, "REJECTED: Max Short Position Limit Breached"
            
        # Check Circuit Breaker Drawdown limits
        if self.cumulative_pnl <= self.max_drawdown_limit:
            return False, "REJECTED: System Circuit Breaker Triggered - Global Drawdown Cap Hit"
            
        return True, "CLEARED"


class OrderFlowImbalanceEngine:
    """Calculates sub-second microstructural alpha vectors from streaming L2 book updates."""
    def __init__(self):
        self.prev_best_bid_p = None
        self.prev_best_bid_q = None
        self.prev_best_ask_p = None
        self.prev_best_ask_q = None

    def calculate_ofi_delta(self, target_payload):
        best_bid_p = float(target_payload['bids'][0][0])
        best_bid_q = float(target_payload['bids'][0][1])
        best_ask_p = float(target_payload['asks'][0][0])
        best_ask_q = float(target_payload['asks'][0][1])

        if self.prev_best_bid_p is None:
            self.prev_best_bid_p, self.prev_best_bid_q = best_bid_p, best_bid_q
            self.prev_best_ask_p, self.prev_best_ask_q = best_ask_p, best_ask_q
            return 0.0

        # Bid Side demand changes
        if best_bid_p > self.prev_best_bid_p: delta_bid = best_bid_q
        elif best_bid_p == self.prev_best_bid_p: delta_bid = best_bid_q - self.prev_best_bid_q
        else: delta_bid = 0.0

        # Ask Side supply changes
        if best_ask_p > self.prev_best_ask_p: delta_ask = 0.0
        elif best_ask_p == self.prev_best_ask_p: delta_ask = best_ask_q - self.prev_best_ask_q
        else: delta_ask = best_ask_q

        ofi_signal = delta_bid - delta_ask
        
        # Cache current values for the next frame
        self.prev_best_bid_p, self.prev_best_bid_q = best_bid_p, best_bid_q
        self.prev_best_ask_p, self.prev_best_ask_q = best_ask_p, best_ask_q

        return ofi_signal


class MasterEventOrchestrator:
    """The core engine pipeline binding the network stream, signal engine, and risk matrix."""
    def __init__(self):
        self.ofi_engine = OrderFlowImbalanceEngine()
        self.risk_engine = RealTimeRiskEngine(max_position_cap=2)
        self.signal_threshold = 0.15 # Minimum BTC imbalance volume required to trigger a trade

    async def run_engine_loop(self):
        print("[ORCHESTRATOR] Initializing Event-Driven Strategy Core System...")
        print("[NETWORKING] Activating secure persistent WebSocket connection layer...")
        
        async with websockets.connect(STREAM_URL) as ws:
            print("[NETWORKING] Pipeline connected. Live trading loop engaged.\n")
            
            packet_count = 0
            while packet_count < 25: # Run sample window to verify execution framework
                raw_msg = await ws.recv()
                packet_data = json.loads(raw_msg)
                target = packet_data.get('data', packet_data)
                
                # 1. Pipeline Pass: Extract mid-price metrics
                mid_p = (float(target['bids'][0][0]) + float(target['asks'][0][0])) / 2.0
                
                # 2. Pipeline Pass: Calculate predictive alpha metrics live
                instant_ofi = self.ofi_engine.calculate_ofi_delta(target)
                
                # 3. Pipeline Pass: Execution Strategy Core Decision Layer
                order_triggered = False
                action_taken = "HOLD"
                risk_status = "N/A"
                
                if instant_ofi > self.signal_threshold:
                    order_triggered = True
                    action_taken = "BUY"
                elif instant_ofi < -self.signal_threshold:
                    order_triggered = True
                    action_taken = "SELL"
                    
                # 4. Pipeline Pass: Route through Pre-Trade Risk Checks if triggered
                if order_triggered:
                    cleared, risk_msg = self.risk_engine.check_risk_clearance(action_taken)
                    risk_status = risk_msg
                    if cleared:
                        # Update localized mock state positions on successful confirmation
                        if action_taken == "BUY": self.risk_engine.current_position += 1
                        else: self.risk_engine.current_position -= 1
                
                # 5. Pipeline Pass: Render high-fidelity dashboard metrics
                print("\033[H\033[J", end="") # Crisp terminal clearing anchor
                print("="*72)
                print(f" PROJECT #037: HIGH-FREQUENCY MASTER EVENT-DRIVEN EXECUTION CORE")
                print("="*72)
                print(f"  Asset Class Target : BTCUSDT        | Live Spot Mid: ${mid_p:.2f}")
                print(f"  System Position Vector : {self.risk_engine.current_position:<3} layers | Cumulative PnL: ${self.risk_engine.cumulative_pnl:+.2f}")
                print(f"  ----------------------------------------------------------------------")
                print(f"  ALPHA SIGNAL GENERATION:")
                print(f"    * Instant Dynamic Imbalance Index (OFI): {instant_ofi:+.4f} BTC")
                print(f"  ----------------------------------------------------------------------")
                print(f"  STRATEGY PIPELINE ACTION REGISTRY:")
                print(f"    * Signal Determination : {action_taken:<4}")
                print(f"    * Risk Management Gate : {risk_status}")
                print("="*72 + "\n")
                
                packet_count += 1
                await asyncio.sleep(0.001)

if __name__ == "__main__":
    orchestrator = MasterEventOrchestrator()
    asyncio.run(orchestrator.run_engine_loop())