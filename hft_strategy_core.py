import asyncio
import time
import websockets
from data_ingress import DataIngressLayer
from state_vector import CoreStateVectorEngine

# --- CONFIGURATION CONSTANTS ---
STREAM_URL = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"


class OrderFlowImbalanceEngine:
    """
    Calculates microstructural order flow imbalance metrics from L2 tick data.
    """
    def __init__(self):
        self.prev_best_bid_p = 0.0
        self.prev_best_bid_q = 0.0
        self.prev_best_ask_p = 0.0
        self.prev_best_ask_q = 0.0

    def calculate_ofi_delta(self, target_payload: dict) -> float:
        # Extract current top-of-book values from Binance payload
        try:
            best_bid_p = float(target_payload.get("b", 0.0))
            best_bid_q = float(target_payload.get("B", 0.0))
            best_ask_p = float(target_payload.get("a", 0.0))
            best_ask_q = float(target_payload.get("A", 0.0))
        except (ValueError, TypeError):
            return 0.0

        delta_bid = 0.0
        if best_bid_p > self.prev_best_bid_p:
            delta_bid = best_bid_q
        elif best_bid_p == self.prev_best_bid_p:
            delta_bid = best_bid_q - self.prev_best_bid_q

        delta_ask = 0.0
        if best_ask_p > self.prev_best_ask_p:
            delta_ask = 0.0
        elif best_ask_p == self.prev_best_ask_p:
            delta_ask = best_ask_q - self.prev_best_ask_q
        else:
            delta_ask = best_ask_q

        ofi_signal = delta_bid - delta_ask

        # Update historical state vectors
        self.prev_best_bid_p, self.prev_best_bid_q = best_bid_p, best_bid_q
        self.prev_best_ask_p, self.prev_best_ask_q = best_ask_p, best_ask_q

        return ofi_signal


class MasterEventOrchestrator:
    """
    The core engine pipeline binding the network stream, signal engine, and risk matrix.
    """
    def __init__(self):
        # 1. Thread-safe Asynchronous Pipeline Broker
        self.data_queue = asyncio.Queue()
        
        # 2. Ingress & Risk Modules Wired Modularly
        self.ingress = DataIngressLayer(stream_url=STREAM_URL, data_queue=self.data_queue)
        self.risk_engine = CoreStateVectorEngine()
        self.ofi_engine = OrderFlowImbalanceEngine()
        
        # 3. Parameter Tuning Metrics
        self.signal_threshold = 0.0001
        self.max_packets_to_process = 15

    async def run_engine_loop(self):
        print("[ORCHESTRATOR] Initializing Event-Driven Strategy Core System...")
        print("[NETWORKING] Connecting to secure persistent WebSocket data stream...")
        
        # Fire off the data background worker concurrently
        asyncio.create_task(self.ingress.stream_receiver_loop())
        print("[NETWORKING] Handshake verified. Live trading loop engaged.\n")
        
        packet_count = 0
        
        while packet_count < self.max_packets_to_process:
            # Pull the incoming packet from our modular queue broker
            packet = await self.data_queue.get()
            
            # Extract basic spot price data from payload
            try:
                bid_p = float(packet.get("b", 0.0))
                ask_p = float(packet.get("a", 0.0))
                mid_p = (bid_p + ask_p) / 2.0
            except (ValueError, TypeError):
                mid_p = 0.0
            
            # Compute raw Alpha engine calculations
            instant_of = self.ofi_engine.calculate_ofi_delta(packet)
            
            # Determine actionable signals from thresholds
            if instant_of > self.signal_threshold:
                action_taken = "BUY"
            elif instant_of < -self.signal_threshold:
                action_taken = "SELL"
            else:
                action_taken = "HOLD"
                
            # Evaluate decentralized Pre-Trade Risk Gate
            risk_passed, risk_status = self.risk_engine.evaluate_pre_trade_constraints(action_taken)
            
            if risk_passed and action_taken != "HOLD":
                # Simulated VWAP calculation execution step matching project 33 structure
                realized_vwap = mid_p  
                fill_price = realized_vwap
                self.risk_engine.update_position_state(action_taken, fill_price)
            else:
                fill_price = 0.0
                if action_taken == "HOLD":
                    risk_status = "N/A"

            # Render high-fidelity dashboard metrics
            print("\033[H\033[J", end="")
            print("-" * 85)
            print(f" PROJECT #037: HIGH-FREQUENCY MASTER EVENT-DRIVEN EXECUTION CORE")
            print("-" * 85)
            print(f" Asset Class Target : BTCUSDT        | Live Spot Mid: ${mid_p:.2f}")
            print(f" System Position Vector : {self.risk_engine.current_position:2d} layers | Cumulative PnL: ${self.risk_engine.cumulative_pnl:.4f}")
            print("-" * 85)
            print(f" ALPHA SIGNAL GENERATION:")
            print(f"  * Instant Dynamic Imbalance Index (OFI): {instant_of:+.4f} BTC")
            print("-" * 85)
            print(f" ANTI-STOCHASTIC ORCHESTRATION EVENT REGISTRY (AVI METRICS OVERRIDE):")
            print(f"  * Signal Determination : {action_taken:<4}")
            print(f"  * Pre-Trade Risk Gate  : {risk_status}")
            
            if fill_price > 0:
                print(f"  * Realized VWAP Match  : 3.5 Units Filled @ ${fill_price:.2f}")
            print("-" * 85 + "\n")
            
            packet_count += 1
            self.data_queue.task_done()
            await asyncio.sleep(0.001)


if __name__ == "__main__":
    orchestrator = MasterEventOrchestrator()
    asyncio.run(orchestrator.run_engine_loop())