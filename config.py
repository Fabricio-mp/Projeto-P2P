import uuid
import socket
from logger_config import setup_logger

config_logger = setup_logger("CONFIG")
SERVER_UUID = str(uuid.uuid4())


MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000


WORKER_HOST = "127.0.0.1"
WORKER_PORT = 5000


LOAD_THRESHOLD   = 5    
TASK_DURATION    = 0.5  
REQUEST_INTERVAL = 1.0  


HEARTBEAT_INTERVAL = 2.0  
HEARTBEAT_TIMEOUT  = 5.0  


SPRINT1_HEARTBEAT_ONLY = False


CONNECTION_ERROR_THRESHOLD = 4        
ELECTION_PORT              = 5100     
ELECTION_RETRY_INTERVAL    = 2.0     
ELECTION_CANDIDATES        = ["127.0.0.1"]


NEIGHBOR_MASTERS = [("127.0.0.1", 5000)]


def _self_neighbor_warning():
    try:
        local = {"127.0.0.1", "localhost"}
        local.update(socket.gethostbyname_ex(socket.gethostname())[2])
    except OSError:
        local = {"127.0.0.1", "localhost"}
    for host, port in NEIGHBOR_MASTERS:
        if host in local and port == MASTER_PORT:
            config_logger.warning(
                f"[CONFIG] AVISO: NEIGHBOR_MASTERS aponta para este próprio processo "
                f"({host}:{port}). Pedidos de ajuda vão falhar ou ser auto-enviados. "
                "Configure o IP real do vizinho para uso em producao."
            )


_self_neighbor_warning()