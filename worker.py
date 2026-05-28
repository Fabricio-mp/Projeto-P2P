import socket
import time
import uuid
import json
import sys
from logger_config import worker_logger
import os
import shutil
import subprocess
import threading

from config import (
    HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, MASTER_PORT,
    TASK_DURATION, CONNECTION_ERROR_THRESHOLD,
    ELECTION_PORT, ELECTION_RETRY_INTERVAL, ELECTION_CANDIDATES,
    WORKER_HOST, WORKER_PORT,
)

WORKER_UUID = str(uuid.uuid4())

master_target      = {"host": None, "port": None}
master_target_lock = threading.Lock()

master_process      = None
master_process_lock = threading.Lock()

original_master_target        = None
original_master_uuid          = None
current_master_uuid           = None
last_registration_master_uuid = None

_redirect_event  = threading.Event()
_redirect_target = {"host": None, "port": None}

_processed_task_recently = False




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
            worker_logger.info("Falha no parser P2P: campos obrigatorios ausentes.")
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


def receive_with_timeout(sock, timeout_seconds):
    original = sock.gettimeout()
    try:
        sock.settimeout(timeout_seconds)
        return receive(sock)
    except socket.timeout:
        return None
    finally:
        sock.settimeout(original)




def _ts():
    return time.strftime("%H:%M:%S")


def connect(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(HEARTBEAT_TIMEOUT)
    sock.connect((host, port))
    worker_logger.info(f"Conectado ao Master {host}:{port}")
    return sock


def local_addresses():
    hosts = {"127.0.0.1", "localhost"}
    try:
        hosts.update(socket.gethostbyname_ex(socket.gethostname())[2])
    except OSError:
        pass
    return hosts


def is_local_host(host):
    return host in local_addresses()




def set_master_target(host, port, reason=""):
    global original_master_target
    with master_target_lock:
        master_target["host"] = host
        master_target["port"] = port
        if original_master_target is None:
            original_master_target = (host, port)
    if reason:
        worker_logger.info(f"▶ Master alvo atualizado: {host}:{port} ({reason})")


def get_master_target():
    with master_target_lock:
        return master_target["host"], master_target["port"]




def build_presentation_payload():
    current_host, current_port = get_master_target()
    
    if (
        original_master_target is not None
        and original_master_uuid is not None
        and (current_host, current_port) != original_master_target
    ):
        return wrap_p2p_message("register_temporary_worker", {
            "worker_id": WORKER_UUID,
            "original_master_address": f"{original_master_target[0]}:{original_master_target[1]}",
            "SERVER_UUID": original_master_uuid
        })

    return {"WORKER": "ALIVE", "WORKER_UUID": WORKER_UUID}


def register_with_master(sock):
    global original_master_uuid, current_master_uuid, last_registration_master_uuid

    send(sock, build_presentation_payload())
    response = receive(sock)
    if not response:
        return None

    srv_uuid = response.get("SERVER_UUID") or response.get("payload", {}).get("SERVER_UUID")
    if isinstance(srv_uuid, str) and srv_uuid.strip():
        current_master_uuid = srv_uuid
        if original_master_uuid is None:
            original_master_uuid = srv_uuid

    worker_logger.info(f"Apresentado ao Master {current_master_uuid[:8] if current_master_uuid else '?'}.")

    task = response.get("TASK") or response.get("type")
    if task == "QUERY":
        process_task(sock, response.get("payload", response))

    last_registration_master_uuid = current_master_uuid
    return response




def process_task(sock, task_msg):
    global _processed_task_recently
    _processed_task_recently = True
    
    task_id   = task_msg.get("TASK_ID", "SEM_ID")
    user      = task_msg.get("USER", "desconhecido")
    force_nok = bool(task_msg.get("FORCE_NOK", False))
    status    = "NOK" if force_nok else "OK"

    worker_logger.info(f"▶ Processando {task_id} (user={user}, forçar_nok={force_nok})")
    time.sleep(TASK_DURATION)

    send(sock, {"STATUS": status, "TASK": "QUERY", "WORKER_UUID": WORKER_UUID, "TASK_ID": task_id})

  
    got_ack = False
    for _ in range(5):
        ack = receive_with_timeout(sock, 2.0)
        if not ack:
            break
        if ack.get("STATUS") == "ACK":
            got_ack = True
            break
        else:
            handle_master_message(sock, ack)
            if _redirect_event.is_set():
                break

    symbol = "✔" if got_ack else "✘"
    suffix = "" if got_ack else " (sem ACK do Master)"
    worker_logger.info(f"{symbol} {task_id} concluída → {status}{suffix}")


def handle_master_message(sock, raw_msg):
    """Retorna True para continuar o loop, False para reconectar (redirect)."""
    msg = parse_p2p_message(raw_msg)
    if msg is None:
        return True

    msg_type = msg.get("type") or msg.get("TASK")
    payload = msg.get("payload", msg)

    if msg_type == "QUERY":
        process_task(sock, payload)
    elif msg_type in ("NO_TASK", None):
        pass
    elif msg_type == "HEARTBEAT" and payload.get("RESPONSE") == "ALIVE":
        pass
    elif msg_type == "command_redirect":
        new_addr = payload.get("new_master_address", "")
        if isinstance(new_addr, str) and ":" in new_addr:
            new_host, new_port_str = new_addr.split(":", 1)
            new_port = int(new_port_str)
        else:
            new_host = payload.get("NEW_MASTER_HOST")
            new_port = int(payload.get("NEW_MASTER_PORT", MASTER_PORT))
        
        if isinstance(new_host, str) and new_host:
            worker_logger.info(f"▶ Redirecionado para Master {new_host}:{new_port}")
            set_master_target(new_host, new_port, "redirect do Master")
            with master_target_lock:
                _redirect_target["host"] = new_host
                _redirect_target["port"] = new_port
            _redirect_event.set()
            return False
            
    elif msg_type == "command_release":
        orig_addr = payload.get("original_master_address", "")
        if isinstance(orig_addr, str) and ":" in orig_addr:
            orig_host, orig_port_str = orig_addr.split(":", 1)
            orig_port = int(orig_port_str)
            worker_logger.info(f"▶ Liberado pelo Master. Voltando para original {orig_host}:{orig_port}")
            set_master_target(orig_host, orig_port, "liberado do Master temporario")
            with master_target_lock:
                _redirect_target["host"] = orig_host
                _redirect_target["port"] = orig_port
            _redirect_event.set()
            return False
            
    else:
        worker_logger.info(f"Mensagem desconhecida do Master: task={msg_type!r} — ignorada.")

    return True




def get_free_disk_bytes():
    return shutil.disk_usage(os.path.dirname(os.path.abspath(__file__))).free


def ensure_local_master_running():
    global master_process
    with master_process_lock:
        if master_process is not None and master_process.poll() is None:
            return
        master_file    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "master.py")
        master_process = subprocess.Popen(
            [sys.executable, master_file],
            cwd=os.path.dirname(master_file),
        )
        worker_logger.info(f"★ Eleito Master! Iniciando master.py local...")



def unique_candidates():
    seen = dict.fromkeys(h for h in ELECTION_CANDIDATES if h)
    seen.update(dict.fromkeys(local_addresses()))
    return list(seen)


def handle_election_message(conn):
    msg = receive(conn)
    if msg is None:
        conn.close()
        return

    task = msg.get("TASK")

    if task == "ELECTION_QUERY":
        send(conn, {
            "TASK": "ELECTION_RESPONSE",
            "WORKER_UUID": WORKER_UUID,
            "FREE_BYTES": get_free_disk_bytes(),
        })

    elif task == "ELECTION_ANNOUNCE":
        new_host = msg.get("NEW_MASTER_HOST")
        new_port = int(msg.get("NEW_MASTER_PORT", MASTER_PORT))
        if isinstance(new_host, str) and new_host:
            set_master_target(new_host, new_port, "consenso")
            if is_local_host(new_host):
                ensure_local_master_running()
            send(conn, {"TASK": "ACK", "WORKER_UUID": WORKER_UUID})

    conn.close()


def election_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", ELECTION_PORT))
    server.listen(20)
    worker_logger.info(f"Servidor de eleição ativo na porta {ELECTION_PORT}")
    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_election_message, args=(conn,), daemon=True).start()


def query_candidate_disk(host):
    if is_local_host(host):
        return {"host": host, "free_bytes": get_free_disk_bytes()}
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(HEARTBEAT_TIMEOUT)
        sock.connect((host, ELECTION_PORT))
        send(sock, {"TASK": "ELECTION_QUERY", "WORKER_UUID": WORKER_UUID})
        resp = receive(sock)
        sock.close()
        if resp and resp.get("TASK") == "ELECTION_RESPONSE":
            free_bytes = int(resp.get("FREE_BYTES", -1))
            if free_bytes >= 0:
                return {"host": host, "free_bytes": free_bytes}
    except (OSError, ValueError):
        pass
    return None


def announce_winner(host, winner_host, winner_port):
    if is_local_host(host):
        set_master_target(winner_host, winner_port, "consenso local")
        if is_local_host(winner_host):
            ensure_local_master_running()
        return True
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(HEARTBEAT_TIMEOUT)
        sock.connect((host, ELECTION_PORT))
        send(sock, {
            "TASK": "ELECTION_ANNOUNCE",
            "NEW_MASTER_HOST": winner_host,
            "NEW_MASTER_PORT": winner_port,
            "INITIATOR_UUID": WORKER_UUID,
        })
        ack = receive(sock)
        sock.close()
        return bool(ack and ack.get("TASK") == "ACK")
    except OSError:
        return False


def run_master_election():
    candidates = unique_candidates()
    results    = [d for host in candidates if (d := query_candidate_disk(host)) is not None]

    if not results:
        worker_logger.info(f"✘ Eleição falhou: nenhum candidato respondeu.")
        return get_master_target()

    winner      = max(results, key=lambda x: (x["free_bytes"], x["host"]))
    winner_host = winner["host"]
    winner_port = MASTER_PORT
    worker_logger.info(f"★ Eleição: novo Master = {winner_host} ({winner['free_bytes'] // (1024**3)} GB livres)")

    ack_count = sum(1 for host in candidates if announce_winner(host, winner_host, winner_port))
    required  = (len(candidates) // 2) + 1
    symbol    = "✔" if ack_count >= required else "✘"
    label     = "Consenso atingido" if ack_count >= required else "Consenso parcial"
    worker_logger.info(f"{symbol} {label}: {ack_count}/{len(candidates)} ACKs.")

    set_master_target(winner_host, winner_port, "eleicao")
    if is_local_host(winner_host):
        ensure_local_master_running()
    return winner_host, winner_port



def heartbeat_loop(sock):
    """
    Retorna "redirect" se Master enviou command_redirect, "error" em falha.
    Mantém conexão aberta para reagir imediatamente a redirects.
    """
    global _processed_task_recently
    while True:
        _processed_task_recently = False
        if _redirect_event.is_set():
            return "redirect"

        server_uuid_to_send = original_master_uuid if (original_master_target is not None and current_master_uuid != original_master_uuid) else WORKER_UUID
        send(sock, {"TASK": "HEARTBEAT", "SERVER_UUID": server_uuid_to_send})

        msg = receive_with_timeout(sock, HEARTBEAT_INTERVAL)
        if msg is None:
            worker_logger.info(f"✘ Sem resposta do Master no heartbeat.")
            return "error"

        if not handle_master_message(sock, msg):
            return "redirect"
            
        if _redirect_event.is_set():
            return "redirect"

        extra = receive_with_timeout(sock, 0.5)
        if extra is not None:
            if not handle_master_message(sock, extra):
                return "redirect"
                
        if _redirect_event.is_set():
            return "redirect"

        if not _processed_task_recently:
            time.sleep(HEARTBEAT_INTERVAL)


def run(host, port):
    global original_master_uuid, current_master_uuid, last_registration_master_uuid

    set_master_target(host, port, "inicial")
    threading.Thread(target=election_server, daemon=True).start()

    sock               = None
    consecutive_errors = 0

    while True:
        if _redirect_event.is_set():
            _redirect_event.clear()

        target_host, target_port = get_master_target()
        try:
            if sock is None:
                sock = connect(target_host, target_port)

            response = register_with_master(sock)
            if response is None:
                raise TimeoutError("Resposta invalida ou ausente do Master na apresentacao")

            consecutive_errors = 0
            result = heartbeat_loop(sock)

            try:
                sock.close()
            except OSError:
                pass
            sock = None

            if result == "redirect":
                last_registration_master_uuid = None
                worker_logger.info(f"Reconectando ao novo Master...")
                continue

            raise OSError("Heartbeat loop encerrado por erro de conexao")

        except (socket.timeout, TimeoutError, OSError):
            consecutive_errors += 1
            worker_logger.info(f"✘ OFFLINE — tentativa {consecutive_errors}/{CONNECTION_ERROR_THRESHOLD}")
            try:
                if sock is not None:
                    sock.close()
            except OSError:
                pass
            sock                          = None
            last_registration_master_uuid = None

            curr_host, curr_port = get_master_target()
            if original_master_target and (curr_host, curr_port) != original_master_target:
                worker_logger.info(f"Falha no Master temporario. Voltando para Master original.")
                set_master_target(original_master_target[0], original_master_target[1], "falha no temporario")
                consecutive_errors = 0
                continue

            if consecutive_errors >= CONNECTION_ERROR_THRESHOLD:
                worker_logger.info(f"Master inativo. Iniciando eleição...")
                run_master_election()
                consecutive_errors = 0

            time.sleep(ELECTION_RETRY_INTERVAL)




if __name__ == "__main__":
    try:
        if len(sys.argv) >= 3:
            host = sys.argv[1]
            port = int(sys.argv[2])
        else:
            host = WORKER_HOST
            port = WORKER_PORT

        worker_logger.info(f"Worker iniciado | UUID: {WORKER_UUID[:8]}")
        worker_logger.info(f"Conectando a {host}:{port}...")
        run(host, port)
    except KeyboardInterrupt:
        worker_logger.info(f"\nEncerrado pelo worker.")
        sys.exit(0)