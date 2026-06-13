import asyncio
import websockets
import json
import time

STREAM_URL = "wss://stream.binance.com:9443/ws/btcusdt@depth5@100ms"

class RealTimeRiskEngine:
    """Institutional-grade risk layer tracking state vectors before order routing."""
    def __init__(self, max_position_cap=2, max_drawdown_limit=-50.0):
        self.max_position_cap = max_position_cap
        self.max_drawdown_limit = max_drawdown_limit
        self.current_position = 0
        self.cumulative_pnl = 0.0

    def check_risk_clearance(self, prospective_order_side):
        """Evaluates systematic risk boundaries. Returns True if order is safe to route."""
        if prospective_order_side == "BUY" and self.current_position >= self.max_position_cap:
            return False, "REJECTED: Max Long Position Limit Breached"
        if prospective_order_side == "SELL" and self.current_position <= -self.max_position_cap:
            return False, "REJECTED: Max Short Position Limit Breached"
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
        try:
            best_bid_p = float(target_payload['bids'][0][0])
            best_bid_q = float(target_payload['bids'][0][1])
            best_ask_p = float(target_payload['asks'][0][0])
            best_ask_q = float(target_payload['asks'][0][1])
        except (KeyError, IndexError):
            return 0.0

        if self.prev_best_bid_p is None:
            self.prev_best_bid_p, self.prev_best_bid_q = best_bid_p, best_bid_q
            self.prev_best_ask_p, self.prev_best_ask_q = best_ask_p, best_ask_q
            return 0.0

        # Bid Side demand changes
        if best_bid_p > self.prev_best_bid_p: 
            delta_bid = best_bid_q
        elif best_bid_p == self.prev_best_bid_p: 
            delta_bid = best_bid_q - self.prev_best_bid_q
        else: 
            delta_bid = 0.0

        # Ask Side supply changes
        if best_ask_p > self.prev_best_ask_p: 
            delta_ask = 0.0
        elif best_ask_p == self.prev_best_ask_p: 
            delta_ask = best_ask_q - self.prev_best_ask_q
        else: 
            delta_ask = best_ask_q

        ofi_signal = delta_bid - delta_ask
        self.prev_best_bid_p, self.prev_best_bid_q = best_bid_p, best_bid_q
        self.prev_best_ask_p, self.prev_best_ask_q = best_ask_p, best_ask_q

        return ofi_signal


class MasterEventOrchestrator:
    """The core engine pipeline binding the network stream, signal engine, and risk matrix."""
    def __init__(self):
        self.ofi_engine = OrderFlowImbalanceEngine()
        self.risk_engine = RealTimeRiskEngine(max_position_cap=2)
        self.signal_threshold = 0.15 
        self.order_size_units = 3.5  # Institutional block clip to force deep L2 book sweeping

    def simulate_l2_cross_spread_fill(self, action, target_book):
        """Sweeps through actual L2 layers sequentially to calculate real non-linear VWAP and slippage."""
        layers = target_book['asks'] if action == "BUY" else target_book['bids']
        
        remaining_qty = self.order_size_units
        total_notional_cost = 0.0
        baseline_price = float(layers[0][0])  # Top-of-book (L1) price quote

        for price_str, qty_str in layers:
            price = float(price_str)
            qty = float(qty_str)
            
            match_qty = min(remaining_qty, qty)
            total_notional_cost += (match_qty * price)
            remaining_qty -= match_qty
            if remaining_qty <= 0:
                break
                
        if remaining_qty > 0:  # Fill remainder at final layer boundary if book runs thin
            total_notional_cost += (remaining_qty * price)
            
        realized_vwap = total_notional_cost / self.order_size_units
        slippage_bps = (abs(realized_vwap - baseline_price) / baseline_price) * 10000.0
        return realized_vwap, slippage_bps

    async def run_engine_loop(self):
        print("[ORCHESTRATOR] Initializing Event-Driven Strategy Core System...")
        
        packet_count = 0
        max_packets = 25
        
        while packet_count < max_packets:
            try:
                print("[NETWORKING] Connecting to secure persistent WebSocket data stream...")
                async with websockets.connect(STREAM_URL, open_timeout=15) as ws:
                    print("[NETWORKING] Handshake verified. Live trading loop engaged.\n")
                    
                    while packet_count < max_packets:
                        raw_msg = await ws.recv()
                        packet_data = json.loads(raw_msg)
                        target = packet_data.get('data', packet_data)
                        
                        mid_p = (float(target['bids'][0][0]) + float(target['asks'][0][0])) / 2.0
                        instant_ofi = self.ofi_engine.calculate_ofi_delta(target)
                        
                        order_triggered = False
                        action_taken = "HOLD"
                        risk_status = "N/A"
                        fill_price = 0.0
                        
                        if instant_ofi > self.signal_threshold:
                            order_triggered = True
                            action_taken = "BUY"
                        elif instant_ofi < -self.signal_threshold:
                            order_triggered = True
                            action_taken = "SELL"
                            
                        if order_triggered:
                            cleared, risk_msg = self.risk_engine.check_risk_clearance(action_taken)
                            if cleared:
                                realized_vwap, slippage_bps = self.simulate_l2_cross_spread_fill(action_taken, target)
                                risk_status = f"EXECUTED (Slippage: {slippage_bps:.2f} bps)"
                                fill_price = realized_vwap
                                
                                if action_taken == "BUY": 
                                    self.risk_engine.current_position += 1
                                else: 
                                    self.risk_engine.current_position -= 1
                            else:
                                risk_status = risk_msg
                        
                        # Render high-fidelity dashboard metrics
                        print("\033[H\033[J", end="") 
                        print("="*85)
                        print(f" PROJECT #037: HIGH-FREQUENCY MASTER EVENT-DRIVEN EXECUTION CORE")
                        print("="*85)
                        print(f"  Asset Class Target : BTCUSDT        | Live Spot Mid: ${mid_p:.2f}")
                        print(f"  System Position Vector : {self.risk_engine.current_position:<2} layers | Cumulative PnL: ${self.risk_engine.cumulative_pnl:+.2f}")
                        print(f"  ---------------------------------------------------------------------------------")
                        print(f"  ALPHA SIGNAL GENERATION:")
                        print(f"    * Instant Dynamic Imbalance Index (OFI): {instant_ofi:+.4f} BTC")
                        print(f"  ---------------------------------------------------------------------------------")
                        print(f"  ANTI-STOCHASTIC ORCHESTRATION EVENT REGISTRY (AVI METRICS OVERRIDE):")
                        print(f"    * Signal Determination : {action_taken:<4}")
                        print(f"    * Pre-Trade Risk Gate  : {risk_status}")
                        if fill_price > 0:
                            print(f"    * Realized VWAP Match  : 3.5 Units Filled @ ${fill_price:.2f}")
                        print("="*85 + "\n")
                        
                        packet_count += 1
                        await asyncio.sleep(0.001)
                        
            except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError, Exception) as e:
                print(f"\n[WARNING] Network anomaly encountered: {e}")
                print("[RECOVERY] Active reconnection sequence initiated. Cool-down holding pattern (3s)...")
                await asyncio.sleep(3)

if __name__ == "__main__":
    orchestrator = MasterEventOrchestrator()
    asyncio.run(orchestrator.run_engine_loop())