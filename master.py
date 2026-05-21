#!/usr/bin/env python3
# master.py — Aceita Workers, distribui tarefas e negocia ajuda com vizinhos.

import socket
import threading
import time
import uuid
import json
import sys
from logger_config import master_logger

from config import (
    SERVER_UUID, MASTER_HOST, MASTER_PORT,
    LOAD_THRESHOLD, REQUEST_INTERVAL,
    NEIGHBOR_MASTERS, SPRINT1_HEARTBEAT_ONLY,
)

# Estado global
workers          = {}
borrowed_workers = {}
pending          = 0
pending_lock     = threading.Lock()
task_queue       = []
task_queue_lock  = threading.Lock()

# Impede flood de requisições ao vizinho quando saturado
_help_in_progress = threading.Event()


# ── Comunicação P2P ──────────────────────────────────────────

def wrap_p2p_message(msg_type, payload):
    return {
        "type": msg_type,
        "request_id": str(uuid.uuid4()),
        "payload": payload
    }

def parse_p2p_message(msg):
    if not isinstance(msg, dict):
        return None
    if "type" in msg:
        if "request_id" not in msg or "payload" not in msg:
            master_logger.info("[MASTER] Falha no parser P2P: campos obrigatorios ausentes.")
            return None
        return msg
    return msg


def send(sock, payload):
    try:
        sock.sendall((json.dumps(payload) + "\n").encode())
    except OSError:
        pass


_socket_buffers = {}

def receive(sock):
    try:
        data = _socket_buffers.get(sock, b"")
        while b"\n" not in data:
            chunk = sock.recv(4096)
            if not chunk:
                return None
            data += chunk
        parts = data.split(b"\n", 1)
        _socket_buffers[sock] = parts[1] if len(parts) > 1 else b""
        return json.loads(parts[0])
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
        master_logger.info(f"[MASTER] Sem tarefa para o Worker {worker_uuid[:8]}.")
        return
    send(conn, {
        "TASK": "QUERY",
        "USER": task["USER"],
        "TASK_ID": task["TASK_ID"],
        "FORCE_NOK": task["FORCE_NOK"],
        "SERVER_UUID": SERVER_UUID,
    })
    master_logger.info(f"[MASTER] Enviando {task['TASK_ID']} para Worker {worker_uuid[:8]} | USER={task['USER']}")


# ── Atendimento de Workers ───────────────────────────────────

def handle_worker(worker_uuid, conn, first_msg=None):
    global pending
    raw_msg = first_msg
    while True:
        if raw_msg is None:
            raw_msg = receive(conn)

        if raw_msg is None:
            master_logger.info(f"[MASTER] Worker {worker_uuid[:8]} desconectou.")
            workers.pop(worker_uuid, None)
            borrowed_workers.pop(worker_uuid, None)
            conn.close()
            return

        p2p_msg = parse_p2p_message(raw_msg)
        if p2p_msg is None:
            raw_msg = None
            continue
            
        msg_type = p2p_msg.get("type") or p2p_msg.get("TASK", "")
        payload = p2p_msg.get("payload", p2p_msg)

        if msg_type == "HEARTBEAT" or p2p_msg.get("WORKER") == "ALIVE":
            if not valid_heartbeat(p2p_msg):
                master_logger.info("[MASTER] HEARTBEAT/APRESENTACAO invalido: campos obrigatorios ausentes.")
                workers.pop(worker_uuid, None)
                borrowed_workers.pop(worker_uuid, None)
                conn.close()
                return
            if p2p_msg.get("WORKER") == "ALIVE":
                origem = "emprestado" if borrowed_worker(p2p_msg) else "local"
                workers[worker_uuid] = conn
                master_logger.info(f"[MASTER] Worker {origem} {worker_uuid[:8]} apresentado.")
            else:
                send(conn, build_alive_response())
            dispatch_next_task(conn, worker_uuid)

        elif "STATUS" in p2p_msg or msg_type == "QUERY":
            if not valid_status_report(p2p_msg):
                master_logger.info(f"[MASTER] Status invalido de {worker_uuid[:8]}. Encerrando conexao.")
                workers.pop(worker_uuid, None)
                borrowed_workers.pop(worker_uuid, None)
                conn.close()
                return
            task_id = p2p_msg.get("TASK_ID", "SEM_ID")
            status  = p2p_msg.get("STATUS")
            with pending_lock:
                pending = max(0, pending - 1)
            master_logger.info(f"[MASTER] Tarefa {task_id} concluida por {worker_uuid[:8]} com status {status}. Pendentes: {pending}")
            send(conn, {"STATUS": "ACK", "WORKER_UUID": worker_uuid, "TASK_ID": task_id})

        elif msg_type == "task_done":
            task_id = p2p_msg.get("TASK_ID")
            with pending_lock:
                pending = max(0, pending - 1)
            master_logger.info(f"[MASTER] Tarefa {task_id} concluida por {worker_uuid[:8]}. Pendentes: {pending}")

        elif msg_type in ("register_worker", "register_temporary_worker"):
            wid  = payload.get("worker_id", worker_uuid)
            workers[wid] = conn
            if msg_type == "register_temporary_worker":
                orig_addr = payload.get("original_master_address")
                if orig_addr:
                    borrowed_workers[wid] = orig_addr
                master_logger.info(f"[MASTER] Worker temporario {wid[:8]} registrado.")
            else:
                master_logger.info(f"[MASTER] Worker {wid[:8]} registrado.")
            
            # Enviar a primeira tarefa (ou NO_TASK) para finalizar o handshake de apresentação
            dispatch_next_task(conn, wid)

        elif msg_type == "command_redirect":
            master_logger.info(f"[MASTER] command_redirect recebido de {worker_uuid[:8]} — ignorado pelo Master.")

        else:
            master_logger.info(f"[MASTER] Mensagem desconhecida de {worker_uuid[:8]}: {msg_type} — ignorada.")

        raw_msg = None


# ── Loop de aceitação ────────────────────────────────────────

def accept_loop():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((MASTER_HOST, MASTER_PORT))
    server.listen(20)
    master_logger.info(f"[MASTER] Escutando em {MASTER_HOST}:{MASTER_PORT}")

    while True:
        conn, addr = server.accept()
        raw_msg = receive(conn)
        if raw_msg is None:
            conn.close()
            continue

        p2p_msg = parse_p2p_message(raw_msg)
        if p2p_msg is None:
            conn.close()
            continue

        msg_type = p2p_msg.get("type") or p2p_msg.get("TASK", "")
        payload = p2p_msg.get("payload", p2p_msg)
        worker_uuid = payload.get("worker_id") or p2p_msg.get("WORKER_UUID") or p2p_msg.get("SERVER_UUID") or str(uuid.uuid4())

        if msg_type == "HEARTBEAT" or p2p_msg.get("WORKER") == "ALIVE":
            if not valid_heartbeat(p2p_msg):
                master_logger.info(f"[MASTER] HEARTBEAT/APRESENTACAO invalido de {addr}. Conexao encerrada.")
                conn.close()
                continue
            if p2p_msg.get("WORKER") == "ALIVE":
                origem = "emprestado" if borrowed_worker(p2p_msg) else "proprio"
                master_logger.info(f"[MASTER] Worker {origem} {worker_uuid[:8]} apresentou-se de {addr}.")
            else:
                master_logger.info(f"[MASTER] Heartbeat recebido de {worker_uuid[:8]}.")
            threading.Thread(target=handle_worker, args=(worker_uuid, conn, raw_msg), daemon=True).start()

        elif "register" in msg_type:
            workers[worker_uuid] = conn
            if msg_type == "register_temporary_worker":
                orig_addr = payload.get("original_master_address")
                if orig_addr:
                    borrowed_workers[worker_uuid] = orig_addr
                master_logger.info(f"[MASTER] Worker temporario {worker_uuid[:8]} conectado de {addr}.")
            else:
                master_logger.info(f"[MASTER] Worker proprio {worker_uuid[:8]} conectado de {addr}.")
            threading.Thread(target=handle_worker, args=(worker_uuid, conn, raw_msg), daemon=True).start()

        elif msg_type == "request_help":
            if SPRINT1_HEARTBEAT_ONLY:
                master_logger.info("[MASTER] Modo Sprint 1: ignorando request_help.")
                conn.close()
                continue
            req_id = p2p_msg.get("request_id", "")
            handle_help_request(conn, payload, req_id)

        elif msg_type == "notify_worker_returned":
            master_logger.info(f"[MASTER] Vizinho notificou retorno do worker {payload.get('worker_id', '')[:8]}.")
            conn.close()

        elif msg_type == "command_release":
            if SPRINT1_HEARTBEAT_ONLY:
                master_logger.info("[MASTER] Modo Sprint 1: ignorando command_release.")
                conn.close()
                continue
            master_logger.info("[MASTER] Vizinho liberou os workers. Redirecionando de volta.")
            conn.close()

        else:
            master_logger.info(f"[MASTER] Mensagem desconhecida de {addr}: task={msg_type!r} — conexao encerrada.")
            conn.close()


# ── Geração de carga e Devolução ──────────────────────────────

def release_worker(wid, orig_addr):
    conn = workers.get(wid)
    if conn:
        cmd = wrap_p2p_message("command_release", {"original_master_address": orig_addr})
        send(conn, cmd)
        workers.pop(wid, None)
    borrowed_workers.pop(wid, None)
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        sock.connect((orig_addr["host"], orig_addr["port"]))
        msg = wrap_p2p_message("notify_worker_returned", {"worker_id": wid})
        send(sock, msg)
        sock.close()
    except OSError:
        pass
    master_logger.info(f"[MASTER] Worker temporario {wid[:8]} liberado e devolvido para {orig_addr['host']}:{orig_addr['port']}.")


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
            master_logger.info(f"[MASTER] Fila cheia ({queue_size}/{QUEUE_CAP}) — aguardando workers...")
            continue

        task_id   = f"TASK-{count:04d}"
        user      = users[count % len(users)]
        force_nok = (count % 5 == 0)
        count    += 1

        enqueue_task(task_id, user, force_nok)

        with pending_lock:
            pending += 1
            current_pending = pending

        master_logger.info(f"[MASTER] {task_id} enfileirada | fila={queue_size+1} pendentes={current_pending}")

        if current_pending > LOAD_THRESHOLD and not _help_in_progress.is_set():
            master_logger.info(f"[MASTER] Saturado ({current_pending} pendentes) — pedindo ajuda ao vizinho...")
            threading.Thread(target=ask_for_help, daemon=True).start()
            
        elif current_pending < (LOAD_THRESHOLD * 0.5):
            if borrowed_workers:
                for wid, orig_addr in list(borrowed_workers.items()):
                    release_worker(wid, orig_addr)


# ── Negociação entre Masters ─────────────────────────────────

def ask_for_help():
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
                req = wrap_p2p_message("request_help", {
                    "master_id": SERVER_UUID,
                    "current_load": pending,
                    "capacity": LOAD_THRESHOLD,
                    "workers_needed": 1,
                    "master_port": MASTER_PORT
                })
                send(sock, req)
                resp = receive(sock)
                sock.close()
                p2p_resp = parse_p2p_message(resp)
                if p2p_resp and p2p_resp.get("type") == "response_accepted":
                    master_logger.info(f"[MASTER] Vizinho {host}:{port} aceitou — worker a caminho.")
                    accepted = True
                    break
            except OSError:
                pass

        if accepted:
            # Espera a fila baixar (o worker emprestado deve ajudar a drenar)
            while True:
                time.sleep(2)
                with pending_lock:
                    if pending <= LOAD_THRESHOLD:
                        break
        else:
            # Se rejeitado, aguarda 5 segundos antes de tentar pedir novamente
            time.sleep(5)
    finally:
        _help_in_progress.clear()


def handle_help_request(conn, payload, req_id):
    if len(workers) > 1:
        w_uuid            = list(workers.keys())[0]
        w_sock            = workers[w_uuid]
        requester_host, _ = conn.getpeername()
        requester_port    = payload.get("master_port", MASTER_PORT)
        master_logger.info(f"[MASTER] Aceitei ajudar. Redirecionando Worker {w_uuid[:8]}.")
        
        resp = {
            "type": "response_accepted",
            "request_id": req_id,
            "payload": {
                "workers_offered": 1,
                "worker_details": {"id": w_uuid, "address": "?"}
            }
        }
        send(conn, resp)
        
        redirect_cmd = wrap_p2p_message("command_redirect", {
            "new_master_address": {"host": requester_host, "port": requester_port}
        })
        send(w_sock, redirect_cmd)
        workers.pop(w_uuid, None)
        borrowed_workers.pop(w_uuid, None)
    else:
        resp = {
            "type": "response_rejected",
            "request_id": req_id,
            "payload": {"reason": "high_load"}
        }
        send(conn, resp)
    conn.close()


# ── Entry point ──────────────────────────────────────────────

if __name__ == "__main__":
    try:
        if len(sys.argv) >= 2:
            MASTER_PORT = int(sys.argv[1])
        if len(sys.argv) >= 3:
            NEIGHBOR_MASTERS = [("127.0.0.1", int(sys.argv[2]))]

        master_logger.info(f"[MASTER] Iniciando | UUID: {SERVER_UUID}")
        master_logger.info(f"[MASTER] Porta local: {MASTER_PORT} | Vizinho: {NEIGHBOR_MASTERS[0][1]}")
        master_logger.info(f"[MASTER] Suba workers com: python worker.py 127.0.0.1 {MASTER_PORT}")

        if SPRINT1_HEARTBEAT_ONLY:
            master_logger.info("[MASTER] Modo Sprint 1 ativo: apenas HEARTBEAT para demonstracao.")
            accept_loop()
        else:
            threading.Thread(target=accept_loop, daemon=True).start()
            load_generator()
    except KeyboardInterrupt:
        master_logger.info("\n[MASTER] Encerrado pelo Master.")
        sys.exit(0)