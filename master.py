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


workers          = {} 
borrowed_workers = {}  
pending          = 0


pending_lock     = threading.Lock()
task_queue_lock  = threading.Lock()
workers_lock     = threading.Lock()

task_queue       = []


_help_in_progress = threading.Event()




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



def _valid_uuid_field(msg, key):
    v = msg.get(key)
    return isinstance(v, str) and bool(v.strip())


def valid_heartbeat(msg):
    if not isinstance(msg, dict):
        return False
    if msg.get("TASK") == "HEARTBEAT":
        return _valid_uuid_field(msg, "SERVER_UUID")
    if str(msg.get("WORKER")).lower() == "alive":
        return _valid_uuid_field(msg, "WORKER_UUID")
    return False


def valid_status_report(msg):
    if not isinstance(msg, dict):
        return False
    if not all(k in msg for k in ("STATUS", "TASK", "WORKER_UUID")):
        return False
    if msg.get("STATUS") not in {"OK", "NOK"} or str(msg.get("TASK")).lower() != "query":
        return False
    return _valid_uuid_field(msg, "WORKER_UUID")




def build_alive_response():
    return {"SERVER_UUID": SERVER_UUID, "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}


def borrowed_worker(msg):
    srv = msg.get("SERVER_UUID")
    return isinstance(srv, str) and srv.strip() and srv != SERVER_UUID




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




def handle_worker(worker_uuid, conn, first_msg=None):
    global pending
    raw_msg = first_msg
    while True:
        if raw_msg is None:
            raw_msg = receive(conn)

        if raw_msg is None:
            master_logger.info(f"[MASTER] Worker {worker_uuid[:8]} desconectou.")
            with workers_lock:
                workers.pop(worker_uuid, None)
                borrowed_workers.pop(worker_uuid, None)
            conn.close()
            return

        p2p_msg = parse_p2p_message(raw_msg)
        if p2p_msg is None:
            raw_msg = None
            continue
            
        msg_type = str(p2p_msg.get("type", p2p_msg.get("TASK", ""))).lower()
        payload = p2p_msg.get("payload", p2p_msg)

        if msg_type == "heartbeat" or str(p2p_msg.get("WORKER")).lower() == "alive":
            if not valid_heartbeat(p2p_msg):
                master_logger.info(f"[MASTER] Heartbeat invalido de {worker_uuid[:8]}. Encerrando conexao.")
                with workers_lock:
                    workers.pop(worker_uuid, None)
                    borrowed_workers.pop(worker_uuid, None)
                conn.close()
                return
            if str(p2p_msg.get("WORKER")).lower() == "alive":
                with workers_lock:
                    workers[worker_uuid] = conn
                pass
            else:
                send(conn, build_alive_response())
            
            dispatch_next_task(conn, worker_uuid)

        elif "STATUS" in p2p_msg or msg_type == "query":
            if not valid_status_report(p2p_msg):
                master_logger.info(f"[MASTER] Status invalido de {worker_uuid[:8]}. Encerrando conexao.")
                with workers_lock:
                    workers.pop(worker_uuid, None)
                    borrowed_workers.pop(worker_uuid, None)
                conn.close()
                return
            task_id = p2p_msg.get("TASK_ID", "SEM_ID")
            status  = p2p_msg.get("STATUS")
            with pending_lock:
                pending = max(0, pending - 1)
            origem = "(Worker Emprestado) " if worker_uuid in borrowed_workers else ""
            master_logger.info(f"[MASTER] Tarefa {task_id} concluida por {origem}{worker_uuid[:8]} com status {status}. Pendentes: {pending}")
            send(conn, {"STATUS": "ACK", "WORKER_UUID": worker_uuid, "TASK_ID": task_id})

        elif msg_type == "task_done":
            task_id = p2p_msg.get("TASK_ID")
            with pending_lock:
                pending = max(0, pending - 1)
            master_logger.info(f"[MASTER] Tarefa {task_id} concluida por {worker_uuid[:8]}. Pendentes: {pending}")

        elif msg_type in ("register_worker", "register_temporary_worker"):
            wid  = payload.get("worker_id", worker_uuid)
            with workers_lock:
                workers[wid] = conn
                if msg_type == "register_temporary_worker":
                    orig_addr = payload.get("original_master_address")
                    if orig_addr:
                        borrowed_workers[wid] = orig_addr
                    master_logger.info(f"[MASTER] Worker temporario {wid[:8]} registrado.")
                else:
                    master_logger.info(f"[MASTER] Worker {wid[:8]} registrado.")
            
          
            dispatch_next_task(conn, wid)

        elif msg_type == "command_redirect":
            master_logger.info(f"[MASTER] command_redirect recebido de {worker_uuid[:8]} — ignorado pelo Master.")

        else:
            master_logger.info(f"[MASTER] Mensagem desconhecida de {worker_uuid[:8]}: {msg_type} — ignorada.")

        raw_msg = None




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

        msg_type_raw = p2p_msg.get("type") or p2p_msg.get("TASK") or p2p_msg.get("WORKER")
        if not msg_type_raw:
            conn.close()
            continue
        msg_type = str(msg_type_raw).lower()
        payload = p2p_msg.get("payload", p2p_msg)
        worker_uuid = payload.get("worker_id") or p2p_msg.get("WORKER_UUID") or p2p_msg.get("SERVER_UUID") or str(uuid.uuid4())

        sprint03_types = {"request_help", "response_accepted", "response_rejected", "command_redirect", "register_temporary_worker", "command_release", "notify_worker_returned"}
        if msg_type in sprint03_types:
            if "request_id" not in p2p_msg or "payload" not in p2p_msg:
                master_logger.error(f"[MASTER] Erro de strict parsing: Mensagem '{msg_type}' incompleta (sem request_id ou payload).")
                conn.close()
                continue

        if msg_type == "heartbeat" or str(p2p_msg.get("WORKER")).lower() == "alive":
            if not valid_heartbeat(p2p_msg):
                master_logger.info(f"[MASTER] HEARTBEAT/APRESENTACAO invalido de {addr}. Conexao encerrada.")
                conn.close()
                continue
            if str(p2p_msg.get("WORKER")).lower() == "alive":
                origem = "emprestado" if borrowed_worker(p2p_msg) else "proprio"
                master_logger.info(f"[MASTER] Worker {origem} {worker_uuid[:8]} apresentou-se de {addr}.")
            else:
                master_logger.info(f"[MASTER] Heartbeat recebido de {worker_uuid[:8]}.")
            threading.Thread(target=handle_worker, args=(worker_uuid, conn, raw_msg), daemon=True).start()

        elif "register" in msg_type:
            with workers_lock:
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




def release_worker(wid, orig_addr):
    with workers_lock:
        conn = workers.get(wid)
    if conn:
        cmd = wrap_p2p_message("command_release", {"original_master_address": orig_addr})
        send(conn, cmd)
        with workers_lock:
            workers.pop(wid, None)
    with workers_lock:
        borrowed_workers.pop(wid, None)
    
    try:
        orig_host, orig_port = orig_addr.split(":")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        sock.connect((orig_host, int(orig_port)))
        msg = wrap_p2p_message("notify_worker_returned", {"worker_id": wid})
        send(sock, msg)
        sock.close()
    except OSError:
        pass
    master_logger.info(f"[MASTER] Worker temporario {wid[:8]} liberado e devolvido para {orig_addr}.")

def release_borrowed_workers():
    with workers_lock:
        if borrowed_workers:
            for wid, orig_addr in list(borrowed_workers.items()):
                release_worker(wid, orig_addr)


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
            release_borrowed_workers()



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
                    "workers_needed": 2,
                    "master_port": MASTER_PORT
                })
                send(sock, req)
                resp = receive(sock)
                sock.close()
                p2p_resp = parse_p2p_message(resp)
                
                resp_type_raw = p2p_resp.get("type") if p2p_resp else None
                if not resp_type_raw:
                    continue
                resp_type = str(resp_type_raw).lower()
                
                if "request_id" not in p2p_resp or "payload" not in p2p_resp:
                    master_logger.error(f"[MASTER] Erro de strict parsing: Resposta '{resp_type}' sem request_id ou payload. Descartando.")
                    continue

                if resp_type == "response_accepted":
                    master_logger.info(f"[MASTER] Vizinho {host}:{port} aceitou — worker a caminho.")
                    accepted = True
                    break
                elif resp_type == "response_rejected":
                    reason = p2p_resp.get("payload", {}).get("reason", "desconhecido")
                    master_logger.info(f"[MASTER] Vizinho {host}:{port} recusou ajuda (motivo: {reason}).")
            except socket.timeout:
                master_logger.warning(f"[MASTER] Timeout ao aguardar resposta do vizinho {host}:{port}. Descartando request.")
            except OSError:
                pass

        if accepted:
            
            while True:
                time.sleep(2)
                with pending_lock:
                    if pending <= LOAD_THRESHOLD:
                        break
        else:
            
            time.sleep(5)
    finally:
        _help_in_progress.clear()


def handle_help_request(conn, payload, req_id):
    needed = payload.get("workers_needed", 1)
    
    with workers_lock:
        
        available_to_lend = max(0, len(workers) - 1)
        to_lend = min(needed, available_to_lend)
        
        if to_lend > 0:
            requester_host, _ = conn.getpeername()
            requester_port    = payload.get("master_port", MASTER_PORT)
            
            offered_workers = []
            workers_keys = list(workers.keys())[:to_lend]
            
            for w_uuid in workers_keys:
                offered_workers.append({"id": w_uuid, "address": "?"})
                
            master_logger.info(f"[MASTER] Aceitei ajudar. Redirecionando {to_lend} Worker(s).")
            
            resp = {
                "type": "response_accepted",
                "request_id": req_id,
                "payload": {
                    "workers_offered": to_lend,
                    "worker_details": offered_workers
                }
            }
            send(conn, resp)
            
            for w_uuid in workers_keys:
                w_sock = workers[w_uuid]
                redirect_cmd = wrap_p2p_message("command_redirect", {
                    "new_master_address": f"{requester_host}:{requester_port}"
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




if __name__ == "__main__":
    try:
        if len(sys.argv) >= 2:
            MASTER_PORT = int(sys.argv[1])
        if len(sys.argv) >= 3:
            NEIGHBOR_MASTERS = [("127.0.0.1", int(sys.argv[2]))]
            
        
        if len(sys.argv) == 1:
            test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                test_sock.bind((MASTER_HOST, 5000))
                test_sock.close()
                MASTER_PORT = 5000
                NEIGHBOR_MASTERS = [("127.0.0.1", 5001)]
            except OSError:
                
                test_sock.close()
                MASTER_PORT = 5001
                NEIGHBOR_MASTERS = [("127.0.0.1", 5000)]

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