#!/usr/bin/env python3
# ============================================================
#  master.py
#  - Aceita conexões de Workers (próprios e emprestados)
#  - Gera requisições e distribui tarefas pela fila interna
#  - Monitora saturação e negocia ajuda com Masters vizinhos
# ============================================================

import socket
import threading
import time
import uuid
import json

from config import (
    SERVER_UUID,
    MASTER_HOST,
    MASTER_PORT,
    LOAD_THRESHOLD,
    REQUEST_INTERVAL,
    NEIGHBOR_MASTERS,
    SPRINT1_HEARTBEAT_ONLY,
)

# ── Estado global ────────────────────────────────────────────
workers         = {}               # { worker_uuid: socket }
pending         = 0                # tarefas ainda não concluídas
pending_lock    = threading.Lock()
task_queue      = []               # fila de tarefas aguardando worker disponível
task_queue_lock = threading.Lock()

# ── Guard de pedido de ajuda ─────────────────────────────────
# Garante que apenas UMA thread ask_for_help rode por vez,
# evitando flood de requisições ao vizinho quando saturado.
_help_in_progress = threading.Event()


# ════════════════════════════════════════════════════════════
#  COMUNICAÇÃO
# ════════════════════════════════════════════════════════════

def send(sock, payload):
    """Serializa payload como JSON e envia pela socket (terminador \\n)."""
    try:
        sock.sendall((json.dumps(payload) + "\n").encode())
    except OSError:
        pass


def receive(sock):
    """Lê da socket até encontrar \\n e retorna o objeto JSON desserializado."""
    try:
        data = b""
        while b"\n" not in data:
            chunk = sock.recv(4096)
            if not chunk:
                return None
            data += chunk
        return json.loads(data.split(b"\n")[0])
    except Exception:
        return None


# ════════════════════════════════════════════════════════════
#  VALIDAÇÃO DE MENSAGENS
# ════════════════════════════════════════════════════════════

def valid_heartbeat(msg):
    """Valida mensagens de heartbeat e de apresentação de Worker."""
    if not isinstance(msg, dict):
        return False
    if msg.get("TASK") == "HEARTBEAT":
        server_uuid = msg.get("SERVER_UUID")
        if not isinstance(server_uuid, str) or not server_uuid.strip():
            return False
        return True
    if msg.get("WORKER") == "ALIVE":
        worker_uuid = msg.get("WORKER_UUID")
        if not isinstance(worker_uuid, str) or not worker_uuid.strip():
            return False
        return True
    return False


def valid_status_report(msg):
    """Valida mensagens de conclusão de tarefa enviadas pelo Worker."""
    if not isinstance(msg, dict):
        return False
    for key in ("STATUS", "TASK", "WORKER_UUID"):
        if key not in msg:
            return False
    if msg.get("STATUS") not in {"OK", "NOK"}:
        return False
    if msg.get("TASK") != "QUERY":
        return False
    worker_uuid = msg.get("WORKER_UUID")
    if not isinstance(worker_uuid, str) or not worker_uuid.strip():
        return False
    return True


# ════════════════════════════════════════════════════════════
#  UTILITÁRIOS
# ════════════════════════════════════════════════════════════

def build_alive_response():
    """Monta o payload padrão de resposta a um heartbeat recebido."""
    return {"SERVER_UUID": SERVER_UUID, "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}


def borrowed_worker(msg):
    """Retorna True se o Worker veio de outro Master (SERVER_UUID diferente)."""
    server_uuid = msg.get("SERVER_UUID")
    return isinstance(server_uuid, str) and server_uuid.strip() and server_uuid != SERVER_UUID


# ════════════════════════════════════════════════════════════
#  FILA DE TAREFAS
# ════════════════════════════════════════════════════════════

def enqueue_task(task_id, user, force_nok=False):
    """Adiciona uma tarefa à fila interna de processamento."""
    with task_queue_lock:
        task_queue.append({"TASK_ID": task_id, "USER": user, "FORCE_NOK": force_nok})


def dequeue_task():
    """Remove e retorna o próximo item da fila (FIFO). Retorna None se vazia."""
    with task_queue_lock:
        if not task_queue:
            return None
        return task_queue.pop(0)


def dispatch_next_task(conn, worker_uuid):
    """Envia a próxima tarefa da fila ao Worker. Se não houver, envia NO_TASK."""
    task = dequeue_task()
    if task is None:
        send(conn, {"TASK": "NO_TASK", "SERVER_UUID": SERVER_UUID})
        print(f"[MASTER] Sem tarefa para o Worker {worker_uuid[:8]}.")
        return
    send(
        conn,
        {
            "TASK": "QUERY",
            "USER": task["USER"],
            "TASK_ID": task["TASK_ID"],
            "FORCE_NOK": task["FORCE_NOK"],
            "SERVER_UUID": SERVER_UUID,
        },
    )
    print(f"[MASTER] Enviando {task['TASK_ID']} para Worker {worker_uuid[:8]} | USER={task['USER']}")


# ════════════════════════════════════════════════════════════
#  GERENCIAMENTO DE WORKERS
# ════════════════════════════════════════════════════════════

def handle_worker(worker_uuid, conn, first_msg=None):
    """Loop principal de atendimento de um Worker conectado."""
    global pending
    msg = first_msg
    while True:
        if msg is None:
            msg = receive(conn)

        # Conexão encerrada pelo Worker
        if msg is None:
            print(f"[MASTER] Worker {worker_uuid[:8]} desconectou.")
            workers.pop(worker_uuid, None)
            conn.close()
            return

        # ── Heartbeat ou apresentação ────────────────────────
        if msg.get("TASK") == "HEARTBEAT" or msg.get("WORKER") == "ALIVE":
            if not valid_heartbeat(msg):
                print("[MASTER] HEARTBEAT/APRESENTACAO invalido: campos obrigatorios ausentes.")
                workers.pop(worker_uuid, None)
                conn.close()
                return
            if msg.get("WORKER") == "ALIVE":
                origem = "emprestado" if borrowed_worker(msg) else "local"
                workers[worker_uuid] = conn
                print(f"[MASTER] Worker {origem} {worker_uuid[:8]} apresentado.")
                dispatch_next_task(conn, worker_uuid)
            else:
                send(conn, build_alive_response())
                dispatch_next_task(conn, worker_uuid)

        # ── Relatório de conclusão de tarefa ─────────────────
        elif "STATUS" in msg or msg.get("TASK") == "QUERY":
            if not valid_status_report(msg):
                print(f"[MASTER] Status invalido de {worker_uuid[:8]}. Encerrando conexao.")
                workers.pop(worker_uuid, None)
                conn.close()
                return
            task_id = msg.get("TASK_ID", "SEM_ID")
            status  = msg.get("STATUS")
            with pending_lock:
                pending = max(0, pending - 1)
            print(f"[MASTER] Tarefa {task_id} concluida por {worker_uuid[:8]} com status {status}. Pendentes: {pending}")
            send(conn, {"STATUS": "ACK", "WORKER_UUID": worker_uuid, "TASK_ID": task_id})

        # ── Formato legado de conclusão ───────────────────────
        elif msg.get("TASK") == "task_done":
            task_id = msg.get("TASK_ID")
            with pending_lock:
                pending = max(0, pending - 1)
            print(f"[MASTER] Tarefa {task_id} concluida por {worker_uuid[:8]}. Pendentes: {pending}")

        # ── Registro de Worker (próprio ou temporário) ────────
        elif msg.get("TASK") in ("register_worker", "register_temporary_worker"):
            wid  = msg.get("WORKER_UUID", worker_uuid)
            workers[wid] = conn
            kind = "temporario " if "temporary" in msg.get("TASK", "") else ""
            print(f"[MASTER] Worker {kind}{wid[:8]} registrado.")

        # ── Redirecionamento recebido (Worker emprestado voltando) ──
        elif msg.get("TASK") == "command_redirect":
            # Master recebendo redirect é incomum; apenas loga e ignora.
            print(f"[MASTER] command_redirect recebido de {worker_uuid[:8]} — ignorado pelo Master.")

        else:
            print(f"[MASTER] Mensagem desconhecida de {worker_uuid[:8]}: {msg.get('TASK', '?')} — ignorada.")

        msg = None


# ════════════════════════════════════════════════════════════
#  LOOP DE ACEITAÇÃO
# ════════════════════════════════════════════════════════════

def accept_loop():
    """Escuta na porta MASTER_PORT e despacha cada conexão recebida."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((MASTER_HOST, MASTER_PORT))
    server.listen(20)
    print(f"[MASTER] Escutando em {MASTER_HOST}:{MASTER_PORT}")

    while True:
        conn, addr = server.accept()
        msg = receive(conn)
        if msg is None:
            conn.close()
            continue

        task        = msg.get("TASK", "")
        worker_uuid = msg.get("WORKER_UUID") or msg.get("SERVER_UUID") or str(uuid.uuid4())

        # ── Heartbeat ou apresentação de Worker ──────────────
        if task == "HEARTBEAT" or msg.get("WORKER") == "ALIVE":
            if not valid_heartbeat(msg):
                print(f"[MASTER] HEARTBEAT/APRESENTACAO invalido de {addr}. Conexao encerrada.")
                conn.close()
                continue
            if msg.get("WORKER") == "ALIVE":
                origem = "emprestado" if borrowed_worker(msg) else "proprio"
                print(f"[MASTER] Worker {origem} {worker_uuid[:8]} apresentou-se de {addr}.")
            else:
                print(f"[MASTER] Heartbeat recebido de {worker_uuid[:8]}.")
            threading.Thread(target=handle_worker, args=(worker_uuid, conn, msg), daemon=True).start()

        # ── Registro direto de Worker ────────────────────────
        elif "register" in task:
            workers[worker_uuid] = conn
            kind = "temporario" if "temporary" in task else "proprio"
            print(f"[MASTER] Worker {kind} {worker_uuid[:8]} conectado de {addr}.")
            threading.Thread(target=handle_worker, args=(worker_uuid, conn, msg), daemon=True).start()

        # ── Pedido de ajuda de Master vizinho ────────────────
        elif task == "request_help":
            if SPRINT1_HEARTBEAT_ONLY:
                print("[MASTER] Modo Sprint 1: ignorando request_help.")
                conn.close()
                continue
            handle_help_request(conn, msg)

        # ── Liberação de Workers emprestados ─────────────────
        elif task == "command_release":
            if SPRINT1_HEARTBEAT_ONLY:
                print("[MASTER] Modo Sprint 1: ignorando command_release.")
                conn.close()
                continue
            print("[MASTER] Vizinho liberou os workers. Redirecionando de volta.")
            conn.close()

        else:
            print(f"[MASTER] Mensagem desconhecida de {addr}: task={task!r} — conexao encerrada.")
            conn.close()


# ════════════════════════════════════════════════════════════
#  GERAÇÃO DE CARGA
# ════════════════════════════════════════════════════════════

def load_generator():
    """Gera tarefas periodicamente e as enfileira para os Workers processarem."""
    global pending
    users = ["User1", "User2", "User3", "User4"]
    count = 0
    # Teto da fila: evita crescimento infinito quando workers nao conseguem
    # consumir na mesma velocidade que o gerador produz.
    QUEUE_CAP = LOAD_THRESHOLD * 4

    while True:
        time.sleep(REQUEST_INTERVAL)

        with task_queue_lock:
            queue_size = len(task_queue)

        if queue_size >= QUEUE_CAP:
            # Fila cheia: pausa a geracao e aguarda os workers drenarem.
            print(f"[MASTER] Fila cheia ({queue_size}/{QUEUE_CAP}) — aguardando workers...")
            continue

        task_id   = f"TASK-{count:04d}"
        user      = users[count % len(users)]
        force_nok = (count % 5 == 0)
        count    += 1

        enqueue_task(task_id, user, force_nok)

        with pending_lock:
            pending += 1
            current_pending = pending

        print(f"[MASTER] {task_id} enfileirada | fila={queue_size+1} pendentes={current_pending}")

        if current_pending > LOAD_THRESHOLD and not _help_in_progress.is_set():
            print(f"[MASTER] Saturado ({current_pending} pendentes) — pedindo ajuda ao vizinho...")
            threading.Thread(target=ask_for_help, daemon=True).start()


# ════════════════════════════════════════════════════════════
#  NEGOCIAÇÃO ENTRE MASTERS
# ════════════════════════════════════════════════════════════

def ask_for_help():
    """
    Contata Masters vizinhos solicitando empréstimo de Workers.
    O guard _help_in_progress permanece ativo enquanto o sistema estiver
    saturado — só é liberado quando pending cair abaixo de LOAD_THRESHOLD,
    evitando flood de requisições repetidas ao vizinho.
    """
    if _help_in_progress.is_set():
        return
    _help_in_progress.set()

    try:
        # Tenta cada vizinho uma vez
        accepted = False
        for (host, port) in NEIGHBOR_MASTERS:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                sock.connect((host, port))
                send(sock, {"SERVER_UUID": SERVER_UUID, "TASK": "request_help", "MASTER_PORT": MASTER_PORT})
                resp = receive(sock)
                sock.close()
                if resp and resp.get("TASK") == "response_accepted":
                    print(f"[MASTER] Vizinho {host}:{port} aceitou — worker a caminho.")
                    accepted = True
                    break
            except OSError:
                pass  # vizinho inacessível — tenta o próximo

        if not accepted:
            # Nenhum vizinho pôde ajudar agora. Aguarda até a saturação baixar
            # antes de liberar o guard, evitando disparar pedidos repetidos.
            while True:
                time.sleep(2)
                with pending_lock:
                    still_saturated = pending > LOAD_THRESHOLD
                if not still_saturated:
                    break
    finally:
        _help_in_progress.clear()


def handle_help_request(conn, msg):
    """Responde a um pedido de ajuda: empresta 1 Worker ou rejeita se não houver."""
    if len(workers) > 1:
        w_uuid            = list(workers.keys())[0]
        w_sock            = workers[w_uuid]
        requester_host, _ = conn.getpeername()
        requester_port    = msg.get("MASTER_PORT", MASTER_PORT)

        print(f"[MASTER] Aceitei ajudar. Redirecionando Worker {w_uuid[:8]}.")
        send(conn, {"SERVER_UUID": SERVER_UUID, "TASK": "response_accepted", "WORKERS_TO_SEND": 1})
        send(w_sock, {"TASK": "command_redirect", "NEW_MASTER_HOST": requester_host, "NEW_MASTER_PORT": requester_port})
        workers.pop(w_uuid, None)
    else:
        # Rejeição silenciosa — é o comportamento esperado com poucos workers
        send(conn, {"SERVER_UUID": SERVER_UUID, "TASK": "response_rejected"})
    conn.close()


# ════════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"[MASTER] Iniciando | UUID: {SERVER_UUID}")
    print(f"[MASTER] Suba workers com: python worker.py 127.0.0.1 {MASTER_PORT}")

    if SPRINT1_HEARTBEAT_ONLY:
        print("[MASTER] Modo Sprint 1 ativo: apenas HEARTBEAT para demonstracao.")
        accept_loop()
    else:
        threading.Thread(target=accept_loop, daemon=True).start()
        load_generator()