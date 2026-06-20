# Project #037: High-Frequency Master Event-Driven Execution Core

An institutional-grade, asynchronous event-driven trading infrastructure container designed to process live top-of-book market microstructure data streams, calculate rapid predictive alpha signals, and enforce strict state-vector risk gates.

## 🏗️ Architecture Migration Overview

This repository has been migrated from a single-file monolithic orchestration script into a professional, decoupled **Producer-Consumer multi-file pipeline**. This architectural shift isolates responsibilities across distinct structural pillars to eliminate thread blocking, minimize system jitter, and enforce clear memory state boundaries.
┌──────────────────────────────┐
              │   Live Binance Book Stream   │
              └──────────────┬───────────────┘
                             │ (WebSockets)
                             ▼
              ┌──────────────────────────────┐
              │     data_ingress.py          │
              │  [DataIngressLayer Module]   │
              └──────────────┬───────────────┘
                             │ (Async Queue Broker)
                             ▼
              ┌──────────────────────────────┐
              │     hft_strategy_core.py     │
              │  [MasterEventOrchestrator]   │
              └──────────────┬───────────────┘
                             │
     ┌───────────────────────┴───────────────────────┐
     ▼                                               ▼
┌──────────────────────────────┐                ┌──────────────────────────────┐
│       state_vector.py        │                │     hft_strategy_core.py     │
│ [CoreStateVectorEngine Risk] │                │ [OrderFlowImbalanceEngine]  │
└──────────────────────────────┘                └──────────────────────────────┘
### 🧬 Core System Modules

1. **Data Ingress Layer (`data_ingress.py`)**
   * Manages low-level connection lifecycles to live exchange WebSockets concurrently via `asyncio.create_task`.
   * Unloads incoming JSON payloads into a thread-safe `asyncio.Queue` buffer, abstracting network I/O overhead away from the core execution loop.
   * Implements automated recovery sequence patterns to handle sudden socket disconnections or network drops gracefully.

2. **Core State Vector Engine (`state_vector.py`)**
   * Acts as an isolated pre-trade risk sentinel, executing strict deterministic circuit-breaker logic (`evaluate_pre_trade_constraints`) before signals hit execution simulated lines.
   * Dynamically updates internal state machine tracking attributes including long/short maximum layer constraints, trailing drawdowns, and inventory vectors modularly.

3. **Alpha Signal Engine (`hft_strategy_core.py`)**
   * Encapsulates the `OrderFlowImbalanceEngine` which calculates raw Microstructural Order Flow Imbalances (OFI) across shifting top-of-book levels in real time to capture predictive signals.

4. **Master Orchestrator Core (`hft_strategy_core.py`)**
   * Coordinates the asynchronous pipeline. It consumes data from the ingress queue broker, passes attributes through the decentralized risk gate, and triggers the simulated execution block loops to output dashboard parameters cleanly.

---

## ⚡ Execution Dashboard

The core engine features a high-fidelity console dashboard that tracks order book liquidity, positioning vectors, pre-trade gates, and microstructural fill analytics live:

```text
-------------------------------------------------------------------------------------
 PROJECT #037: HIGH-FREQUENCY MASTER EVENT-DRIVEN EXECUTION CORE
-------------------------------------------------------------------------------------
 Asset Class Target : BTCUSDT        | Live Spot Mid: $63827.99
 System Position Vector : -3 layers | Cumulative PnL: $0.0000
-------------------------------------------------------------------------------------
 ALPHA SIGNAL GENERATION:
  * Instant Dynamic Imbalance Index (OFI): -0.0176 BTC
-------------------------------------------------------------------------------------
 ANTI-STOCHASTIC ORCHESTRATION EVENT REGISTRY (AVI METRICS OVERRIDE):
  * Signal Determination : SELL
  * Pre-Trade Risk Gate  : PASSED: Risk constraints cleared.
  * Realized VWAP Match  : 3.5 Units Filled @ $63827.99
-------------------------------------------------------------------------------------
🚀 Installation & Verification
Prerequisites
Python 3.10+

websockets library installed

Running the System
Clone the directory, open your terminal inside the project root workspace, and execute the orchestrator:

Bash
pip install websockets
python hft_strategy_core.py

---
