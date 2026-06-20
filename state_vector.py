class CoreStateVectorEngine:
    def __init__(self, max_position_layers: int = 5, max_drawdown_pct: float = 0.02):
        # State Tracking Vectors
        self.current_position = 0
        self.cumulative_pnl = 0.0
        
        # Risk Constraints
        self.max_position_layers = max_position_layers
        self.max_drawdown_pct = max_drawdown_pct
        self.system_halted = False

    def evaluate_pre_trade_constraints(self, action_intent: str) -> tuple[bool, str]:
        """
        Validates internal risk states before passing the signal to the execution engine.
        Returns: (Passed/Failed status, Logging message string)
        """
        if self.system_halted:
            return False, "HALTED: Risk breach active."

        # 1. Check Maximum Layer Constraints
        if action_intent == "BUY" and self.current_position >= self.max_position_layers:
            return False, "REJECTED: Max Long limits exceeded."
            
        if action_intent == "SELL" and self.current_position <= -self.max_position_layers:
            return False, "REJECTED: Max Short limits exceeded."

        return True, "PASSED: Risk constraints cleared."

    def update_position_state(self, action_taken: str, fill_price: float):
        """
        Updates the internal layer state machine upon confirmed order execution.
        """
        if action_taken == "BUY":
            self.current_position += 1
        elif action_taken == "SELL":
            self.current_position -= 1
            
    def update_pnl_state(self, realized_trade_pnl: float):
        """
        Monitors cumulative PnL vectors and triggers circuit breakers if thresholds are breached.
        """
        self.cumulative_pnl += realized_trade_pnl
        
        # Simple drawdown check placeholder
        if self.cumulative_pnl < -1000.0:  # Hardcoded dollar threshold example
            self.system_halted = True