#!/usr/bin/env python3
# worker.py — Conecta ao Master, processa tarefas, detecta falha e inicia eleição.
# Uso: python3 worker.py <host> <porta>
#Exemplo: python worker.py 127.0.0.1 5000

import socket
import time
import uuid
import json
import sys
import os
import shutil
import subprocess
import threading

from config import (
    HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, MASTER_PORT,
    TASK_DURATION, CONNECTION_ERROR_THRESHOLD,
    ELECTION_PORT, ELECTION_RETRY_INTERVAL, ELECTION_CANDIDATES,
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


def receive_with_timeout(sock, timeout_seconds):
    original = sock.gettimeout()
    try:
        sock.settimeout(timeout_seconds)
        return receive(sock)
    except socket.timeout:
        return None
    finally:
        sock.settimeout(original)


# ── Conexão ──────────────────────────────────────────────────

def _ts():
    return time.strftime("%H:%M:%S")


def connect(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(HEARTBEAT_TIMEOUT)
    sock.connect((host, port))
    print(f"[{_ts()}] Conectado ao Master {host}:{port}")
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


# ── Master alvo ──────────────────────────────────────────────

def set_master_target(host, port, reason=""):
    global original_master_target
    with master_target_lock:
        master_target["host"] = host
        master_target["port"] = port
        if original_master_target is None:
            original_master_target = (host, port)
    if reason:
        print(f"[{_ts()}] ▶ Master alvo atualizado: {host}:{port} ({reason})")


def get_master_target():
    with master_target_lock:
        return master_target["host"], master_target["port"]


# ── Apresentação ─────────────────────────────────────────────

def build_presentation_payload():
    current_host, current_port = get_master_target()
    payload = {"WORKER": "ALIVE", "WORKER_UUID": WORKER_UUID}
    if (
        original_master_target is not None
        and original_master_uuid is not None
        and (current_host, current_port) != original_master_target
    ):
        payload["SERVER_UUID"] = original_master_uuid
    return payload


def register_with_master(sock):
    global original_master_uuid, current_master_uuid, last_registration_master_uuid

    send(sock, build_presentation_payload())
    response = receive(sock)
    if not response:
        return None

    srv_uuid = response.get("SERVER_UUID")
    if isinstance(srv_uuid, str) and srv_uuid.strip():
        current_master_uuid = srv_uuid
        if original_master_uuid is None:
            original_master_uuid = srv_uuid

    print(f"[{_ts()}] Apresentado ao Master {current_master_uuid[:8] if current_master_uuid else '?'}.")

    if response.get("TASK") == "QUERY":
        process_task(sock, response)

    last_registration_master_uuid = current_master_uuid
    return response


# ── Processamento de tarefas ─────────────────────────────────

def process_task(sock, task_msg):
    task_id   = task_msg.get("TASK_ID", "SEM_ID")
    user      = task_msg.get("USER", "desconhecido")
    force_nok = bool(task_msg.get("FORCE_NOK", False))
    status    = "NOK" if force_nok else "OK"

    print(f"[{_ts()}] ▶ Processando {task_id} (user={user}, forçar_nok={force_nok})")
    time.sleep(TASK_DURATION)

    send(sock, {"STATUS": status, "TASK": "QUERY", "WORKER_UUID": WORKER_UUID, "TASK_ID": task_id})

    ack = receive(sock)
    symbol = "✔" if ack and ack.get("STATUS") == "ACK" else "✘"
    suffix = "" if ack and ack.get("STATUS") == "ACK" else " (sem ACK do Master)"
    print(f"[{_ts()}] {symbol} {task_id} concluída → {status}{suffix}")


def handle_master_message(sock, msg):
    """Retorna True para continuar o loop, False para reconectar (redirect)."""
    if not isinstance(msg, dict):
        return True

    task = msg.get("TASK")

    if task == "QUERY":
        process_task(sock, msg)
    elif task in ("NO_TASK", None):
        pass
    elif task == "HEARTBEAT" and msg.get("RESPONSE") == "ALIVE":
        pass
    elif task == "command_redirect":
        new_host = msg.get("NEW_MASTER_HOST")
        new_port = int(msg.get("NEW_MASTER_PORT", MASTER_PORT))
        if isinstance(new_host, str) and new_host:
            print(f"[{_ts()}] ▶ Redirecionado para Master {new_host}:{new_port}")
            set_master_target(new_host, new_port, "redirect do Master")
            with master_target_lock:
                _redirect_target["host"] = new_host
                _redirect_target["port"] = new_port
            _redirect_event.set()
            return False
    else:
        print(f"[{_ts()}] Mensagem desconhecida do Master: task={task!r} — ignorada.")

    return True


# ── Processo master local ─────────────────────────────────────

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
        print(f"[{_ts()}] ★ Eleito Master! Iniciando master.py local...")


# ── Eleição de Master ────────────────────────────────────────

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
    print(f"[{_ts()}] Servidor de eleição ativo na porta {ELECTION_PORT}")
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
        print(f"[{_ts()}] ✘ Eleição falhou: nenhum candidato respondeu.")
        return get_master_target()

    winner      = max(results, key=lambda x: (x["free_bytes"], x["host"]))
    winner_host = winner["host"]
    winner_port = MASTER_PORT
    print(f"[{_ts()}] ★ Eleição: novo Master = {winner_host} ({winner['free_bytes'] // (1024**3)} GB livres)")

    ack_count = sum(1 for host in candidates if announce_winner(host, winner_host, winner_port))
    required  = (len(candidates) // 2) + 1
    symbol    = "✔" if ack_count >= required else "✘"
    label     = "Consenso atingido" if ack_count >= required else "Consenso parcial"
    print(f"[{_ts()}] {symbol} {label}: {ack_count}/{len(candidates)} ACKs.")

    set_master_target(winner_host, winner_port, "eleicao")
    if is_local_host(winner_host):
        ensure_local_master_running()
    return winner_host, winner_port


# ── Loop principal ───────────────────────────────────────────

def heartbeat_loop(sock):
    """
    Retorna "redirect" se Master enviou command_redirect, "error" em falha.
    Mantém conexão aberta para reagir imediatamente a redirects.
    """
    while True:
        send(sock, {"TASK": "HEARTBEAT", "SERVER_UUID": WORKER_UUID})

        msg = receive_with_timeout(sock, HEARTBEAT_INTERVAL)
        if msg is None:
            print(f"[{_ts()}] ✘ Sem resposta do Master no heartbeat.")
            return "error"

        if not handle_master_message(sock, msg):
            return "redirect"

        extra = receive_with_timeout(sock, 0.5)
        if extra is not None:
            handle_master_message(sock, extra)


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
                print(f"[{_ts()}] Reconectando ao novo Master...")
                continue

            raise OSError("Heartbeat loop encerrado por erro de conexao")

        except (socket.timeout, TimeoutError, OSError):
            consecutive_errors += 1
            print(f"[{_ts()}] ✘ OFFLINE — tentativa {consecutive_errors}/{CONNECTION_ERROR_THRESHOLD}")
            try:
                if sock is not None:
                    sock.close()
            except OSError:
                pass
            sock                          = None
            last_registration_master_uuid = None

            if consecutive_errors >= CONNECTION_ERROR_THRESHOLD:
                print(f"[{_ts()}] Master inativo. Iniciando eleição...")
                run_master_election()
                consecutive_errors = 0

            time.sleep(ELECTION_RETRY_INTERVAL)


# ── Entry point ──────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python worker.py <host> <porta>")
        print("Exemplo: python worker.py 127.0.0.1 5000")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    print(f"[{_ts()}] Worker iniciado | UUID: {WORKER_UUID[:8]}")
    print(f"[{_ts()}] Conectando a {host}:{port}...")
    run(host, port)