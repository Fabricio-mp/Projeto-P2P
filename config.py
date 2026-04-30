#!/usr/bin/env python3
# ============================================================
#  config.py
#  Parâmetros centrais do sistema Master/Worker.
#  Importado por master.py e worker.py.
# ============================================================

import uuid
import socket

# ── Identidade do servidor ───────────────────────────────────
# UUID gerado automaticamente a cada inicialização do processo.
SERVER_UUID = str(uuid.uuid4())

# ── Rede do Master ───────────────────────────────────────────
# Use "0.0.0.0" para aceitar conexões em todas as interfaces locais.
MASTER_HOST = "0.0.0.0"
MASTER_PORT = 5000

# ── Comportamento de tarefas ─────────────────────────────────
# Limite de tarefas pendentes antes de o Master pedir ajuda a um vizinho.
LOAD_THRESHOLD = 5
# Duração simulada (em segundos) do processamento de cada tarefa no Worker.
TASK_DURATION = 3
# Intervalo (em segundos) entre geração de novas tarefas pelo Master.
REQUEST_INTERVAL = 1.0

# ── Heartbeat ────────────────────────────────────────────────
# Intervalo (em segundos) entre envios de heartbeat do Worker ao Master.
HEARTBEAT_INTERVAL = 30.0
# Tempo máximo (em segundos) aguardando resposta antes de considerar conexão perdida.
HEARTBEAT_TIMEOUT = 5.0

# ── Controle de Sprint ───────────────────────────────────────
# False  → Sprint 2: fluxo completo de fila e processamento ativo.
# True   → Sprint 1: apenas HEARTBEAT para demonstração.
SPRINT1_HEARTBEAT_ONLY = False

# ── Reconexão e eleição ──────────────────────────────────────
# Número de falhas consecutivas antes de iniciar eleição de novo Master.
CONNECTION_ERROR_THRESHOLD = 4
# Porta TCP usada exclusivamente para comunicação de eleição entre Workers.
ELECTION_PORT = 5100
# Intervalo (em segundos) entre tentativas de reconexão após falha.
ELECTION_RETRY_INTERVAL = 2.0
# Candidatos a Master na eleição — vence o de maior espaço livre em disco.
ELECTION_CANDIDATES = [
    "127.0.0.1",
]

# ── Masters vizinhos ─────────────────────────────────────────
# Lista de Masters (host, porta) para pedido de ajuda em caso de saturação.
# ATENÇÃO: em produção, configure com o IP real do Master vizinho.
# Exemplo para duas máquinas:
#   Máquina A → NEIGHBOR_MASTERS = [("192.168.1.20", 5000)]
#   Máquina B → NEIGHBOR_MASTERS = [("192.168.1.10", 5000)]
NEIGHBOR_MASTERS = [
    ("127.0.0.1", 5000),
]

# ── Aviso de auto-vizinho ────────────────────────────────────
# Detecta em tempo de importação se o Master está configurado como vizinho
# de si mesmo — situação válida em testes locais, mas indesejada em produção.
def _self_neighbor_warning():
    try:
        local = {"127.0.0.1", "localhost"}
        local.update(socket.gethostbyname_ex(socket.gethostname())[2])
    except OSError:
        local = {"127.0.0.1", "localhost"}
    for host, port in NEIGHBOR_MASTERS:
        if host in local and port == MASTER_PORT:
            print(
                "[CONFIG] AVISO: NEIGHBOR_MASTERS aponta para este próprio processo "
                f"({host}:{port}). Pedidos de ajuda vão falhar ou ser auto-enviados. "
                "Configure o IP real do vizinho para uso em producao."
            )

_self_neighbor_warning()