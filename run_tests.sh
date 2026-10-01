#!/bin/bash
# Comandos para ejecutar los tests del TP01

# Experimentos (requieren túneles 9090 y carga activa)
python3 experiments/experiment_runner.py --scenario experiments/scenarios/broker-failure.yaml
python3 experiments/experiment_runner.py --scenario experiments/scenarios/full-restart.yaml
python3 experiments/experiment_runner.py --scenario experiments/scenarios/rebalance.yaml

# Revisar resultados
cat output/broker-failure/summary.json
cat output/full-restart/summary.json
cat output/rebalance/summary.json
