import logging
import sys
from colorama import init, Fore, Style

# Inicializa o colorama garantindo que as cores sejam resetadas ao final de cada print
init(autoreset=True)

class ColoredFormatter(logging.Formatter):
    def format(self, record):
        # Acessa a mensagem original
        msg = record.getMessage()

        # Remove prefixo [MASTER] ou timestamps caso ainda venham na mensagem,
        # pois o formatador vai adicionar padronizado.
        if msg.startswith("[MASTER] "):
            msg = msg.replace("[MASTER] ", "", 1)
        
        # Lógica de coloração baseada no conteúdo da mensagem e do logger
        color = Fore.WHITE  # Padrão
        
        # SUCCESS / OK -> verde
        if "✔" in msg or (" OK" in msg and "NOK" not in msg) or "aceitou" in msg:
            color = Fore.GREEN
            
        # NOK / ERROR / OFFLINE -> vermelho
        elif "✘" in msg or "NOK" in msg or "OFFLINE" in msg or "invalido" in msg or "falhou" in msg or "falhar" in msg or "desconectou" in msg:
            color = Fore.RED
            
        # PROCESSANDO -> amarelo
        elif "Processando" in msg or "Enviando" in msg or "concluida" in msg:
            color = Fore.YELLOW
            
        # SATURADO / ALERTAS -> vermelho forte
        elif "Saturado" in msg or "cheia" in msg or "AVISO" in msg or "Encerrado" in msg or "ignorad" in msg or "inativo" in msg:
            color = Style.BRIGHT + Fore.RED
            
        # FILA / STATUS -> branco ou cinza
        elif "fila=" in msg or "pendentes=" in msg or "Status" in msg or "Eleição:" in msg or "Eleito" in msg:
            color = Fore.LIGHTBLACK_EX
            
        # HEARTBEAT / CONEXÃO -> azul claro
        elif "Heartbeat" in msg or "Conectando" in msg or "Conectado" in msg or "Apresentado" in msg or "apresentou" in msg or "Iniciando" in msg or "ativo" in msg or "alvo atualizado" in msg or "registrado" in msg:
            color = Fore.LIGHTBLUE_EX
            
        # Para mensagens não correspondidas, usa cor baseada em quem está logando
        elif record.name == "MASTER":
            color = Fore.CYAN
        elif record.name == "WORKER":
            color = Fore.MAGENTA
            
        # Obter o timestamp
        time_str = self.formatTime(record, "%H:%M:%S")
        
        # Formatar a saída final com as cores e o prefixo (para o MASTER)
        prefix = ""
        if record.name == "MASTER":
            prefix = f"{Fore.BLUE}[MASTER]{Style.RESET_ALL} "
        
        return f"{Fore.LIGHTBLACK_EX}[{time_str}]{Style.RESET_ALL} {prefix}{color}{msg}{Style.RESET_ALL}"


def setup_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    
    if not logger.handlers:
        # Handler de console
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredFormatter())
        logger.addHandler(ch)
        
        # Configuração para salvar em arquivo (melhoria opcional para produção)
        # file_handler = logging.FileHandler("app.log", encoding="utf-8")
        # file_handler.setLevel(logging.DEBUG)
        # file_handler.setFormatter(logging.Formatter("[%(asctime)s] [%(name)s] %(message)s", datefmt="%H:%M:%S"))
        # logger.addHandler(file_handler)
        
    return logger

master_logger = setup_logger("MASTER")
worker_logger = setup_logger("WORKER")
