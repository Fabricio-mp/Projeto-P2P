#!/usr/bin/env python3
# master.py — Aceita Workers, distribui tarefas e negocia ajuda com vizinhos.

import socket
import threading
import time
import uuid
import json

from config import (
    SERVER_UUID, MASTER_HOST, MASTER_PORT,
    LOAD_THRESHOLD, REQUEST_INTERVAL,
    NEIGHBOR_MASTERS, SPRINT1_HEARTBEAT_ONLY,
)

# Estado global
workers         = {}
pending         = 0
pending_lock    = threading.Lock()
task_queue      = []
task_queue_lock = threading.Lock()

# Impede flood de requisições ao vizinho quando saturado
_help_in_progress = threading.Event()


# ── Comunicação ──────────────────────────────────────────────

def send(sock, payload):
    try:
        sock.sendall((json.dumps(payload) + "\n").encode())
    except OSError:
        pass


def receive(sock):
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


# ── Validação ────────────────────────────────────────────────

def _valid_uuid_field(msg, key):
    v = msg.get(key)
    return isinstance(v, str) and bool(v.strip())


def valid_heartbeat(msg):
    if not isinstance(msg, dict):
        return False
    if msg.get("TASK") == "HEARTBEAT":
        return _valid_uuid_field(msg, "SERVER_UUID")
    if msg.get("WORKER") == "ALIVE":
        return _valid_uuid_field(msg, "WORKER_UUID")
    return False


def valid_status_report(msg):
    if not isinstance(msg, dict):
        return False
    if not all(k in msg for k in ("STATUS", "TASK", "WORKER_UUID")):
        return False
    if msg.get("STATUS") not in {"OK", "NOK"} or msg.get("TASK") != "QUERY":
        return False
    return _valid_uuid_field(msg, "WORKER_UUID")


# ── Utilitários ──────────────────────────────────────────────

def build_alive_response():
    return {"SERVER_UUID": SERVER_UUID, "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}


def borrowed_worker(msg):
    srv = msg.get("SERVER_UUID")
    return isinstance(srv, str) and srv.strip() and srv != SERVER_UUID


# ── Fila de tarefas ──────────────────────────────────────────

def enqueue_task(task_id, user, force_nok=False):
    with task_queue_lock:
        task_queue.append({"TASK_ID": task_id, "USER": user, "FORCE_NOK": force_nok})


def dequeue_task():
    with task_queue_lock:
        return task_queue.pop(0) if task_queue else None


def dispatch_next_task(conn, worker_uuid):
    task = dequeue_task()
    if task is None:
        send(conn, {"TASK": "NO_TASK", "SERVER_UUID": SERVER_UUID})
        print(f"[MASTER] Sem tarefa para o Worker {worker_uuid[:8]}.")
        return
    send(conn, {
        "TASK": "QUERY",
        "USER": task["USER"],
        "TASK_ID": task["TASK_ID"],
        "FORCE_NOK": task["FORCE_NOK"],
        "SERVER_UUID": SERVER_UUID,
    })
    print(f"[MASTER] Enviando {task['TASK_ID']} para Worker {worker_uuid[:8]} | USER={task['USER']}")


# ── Atendimento de Workers ───────────────────────────────────

def handle_worker(worker_uuid, conn, first_msg=None):
    global pending
    msg = first_msg
    while True:
        if msg is None:
            msg = receive(conn)

        if msg is None:
            print(f"[MASTER] Worker {worker_uuid[:8]} desconectou.")
            workers.pop(worker_uuid, None)
            conn.close()
            return

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
            else:
                send(conn, build_alive_response())
            dispatch_next_task(conn, worker_uuid)

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

        elif msg.get("TASK") == "task_done":
            task_id = msg.get("TASK_ID")
            with pending_lock:
                pending = max(0, pending - 1)
            print(f"[MASTER] Tarefa {task_id} concluida por {worker_uuid[:8]}. Pendentes: {pending}")

        elif msg.get("TASK") in ("register_worker", "register_temporary_worker"):
            wid  = msg.get("WORKER_UUID", worker_uuid)
            workers[wid] = conn
            kind = "temporario " if "temporary" in msg.get("TASK", "") else ""
            print(f"[MASTER] Worker {kind}{wid[:8]} registrado.")

        elif msg.get("TASK") == "command_redirect":
            print(f"[MASTER] command_redirect recebido de {worker_uuid[:8]} — ignorado pelo Master.")

        else:
            print(f"[MASTER] Mensagem desconhecida de {worker_uuid[:8]}: {msg.get('TASK', '?')} — ignorada.")

        msg = None


# ── Loop de aceitação ────────────────────────────────────────

def accept_loop():
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

        elif "register" in task:
            workers[worker_uuid] = conn
            kind = "temporario" if "temporary" in task else "proprio"
            print(f"[MASTER] Worker {kind} {worker_uuid[:8]} conectado de {addr}.")
            threading.Thread(target=handle_worker, args=(worker_uuid, conn, msg), daemon=True).start()

        elif task == "request_help":
            if SPRINT1_HEARTBEAT_ONLY:
                print("[MASTER] Modo Sprint 1: ignorando request_help.")
                conn.close()
                continue
            handle_help_request(conn, msg)

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


# ── Geração de carga ─────────────────────────────────────────

def load_generator():
    global pending
    users     = ["User1", "User2", "User3", "User4"]
    count     = 0
    QUEUE_CAP = LOAD_THRESHOLD * 4

    while True:
        time.sleep(REQUEST_INTERVAL)

        with task_queue_lock:
            queue_size = len(task_queue)

        if queue_size >= QUEUE_CAP:
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


# ── Negociação entre Masters ─────────────────────────────────

def ask_for_help():
    """Guard permanece ativo até pending cair abaixo de LOAD_THRESHOLD."""
    if _help_in_progress.is_set():
        return
    _help_in_progress.set()
    try:
        accepted = False
        for host, port in NEIGHBOR_MASTERS:
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
                pass

        if not accepted:
            while True:
                time.sleep(2)
                with pending_lock:
                    if pending <= LOAD_THRESHOLD:
                        break
    finally:
        _help_in_progress.clear()


def handle_help_request(conn, msg):
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
        send(conn, {"SERVER_UUID": SERVER_UUID, "TASK": "response_rejected"})
    conn.close()


# ── Entry point ──────────────────────────────────────────────

if __name__ == "__main__":
    print(f"[MASTER] Iniciando | UUID: {SERVER_UUID}")
    print(f"[MASTER] Suba workers com: python worker.py 127.0.0.1 {MASTER_PORT}")

    if SPRINT1_HEARTBEAT_ONLY:
        print("[MASTER] Modo Sprint 1 ativo: apenas HEARTBEAT para demonstracao.")
        accept_loop()
    else:
        threading.Thread(target=accept_loop, daemon=True).start()
        load_generator()