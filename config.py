#!/usr/bin/env python3
# config.py — Parâmetros centrais do sistema Master/Worker.

import uuid
import socket

SERVER_UUID = str(uuid.uuid4())

# Rede do Master
MASTER_HOST = "0.0.0.0"
MASTER_PORT = 5000

# Comportamento de tarefas
LOAD_THRESHOLD   = 5    # tarefas pendentes antes de pedir ajuda ao vizinho
TASK_DURATION    = 3    # segundos simulados por tarefa no Worker
REQUEST_INTERVAL = 1.0  # segundos entre geração de tarefas

# Heartbeat
HEARTBEAT_INTERVAL = 30.0  # intervalo entre heartbeats
HEARTBEAT_TIMEOUT  = 5.0   # timeout de resposta

# Sprint 1: True → só HEARTBEAT; False → fluxo completo
SPRINT1_HEARTBEAT_ONLY = False

# Reconexão e eleição
CONNECTION_ERROR_THRESHOLD = 4        # falhas antes de iniciar eleição
ELECTION_PORT              = 5100     # porta de comunicação de eleição
ELECTION_RETRY_INTERVAL    = 2.0     # intervalo entre tentativas
ELECTION_CANDIDATES        = ["127.0.0.1"]

# Masters vizinhos para pedido de ajuda em caso de saturação.
# Em produção: configure com o IP real do vizinho.
NEIGHBOR_MASTERS = [("127.0.0.1", 5000)]


def _self_neighbor_warning():
    try:
        local = {"127.0.0.1", "localhost"}
        local.update(socket.gethostbyname_ex(socket.gethostname())[2])
    except OSError:
        local = {"127.0.0.1", "localhost"}
    for host, port in NEIGHBOR_MASTERS:
        if host in local and port == MASTER_PORT:
            print(
                f"[CONFIG] AVISO: NEIGHBOR_MASTERS aponta para este próprio processo "
                f"({host}:{port}). Pedidos de ajuda vão falhar ou ser auto-enviados. "
                "Configure o IP real do vizinho para uso em producao."
            )


_self_neighbor_warning()