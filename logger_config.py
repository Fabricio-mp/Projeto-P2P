import logging
import sys
from colorama import init, Fore, Style


init(autoreset=True)

class ColoredFormatter(logging.Formatter):
    def format(self, record):
       
        msg = record.getMessage()

        
        if msg.startswith("[MASTER] "):
            msg = msg.replace("[MASTER] ", "", 1)
        
        
        color = Fore.WHITE  
        
    
        if "✔" in msg or (" OK" in msg and "NOK" not in msg) or "aceitou" in msg:
            color = Fore.GREEN
            
        
        elif "✘" in msg or "NOK" in msg or "OFFLINE" in msg or "invalido" in msg or "falhou" in msg or "falhar" in msg or "desconectou" in msg:
            color = Fore.RED
            
 
        elif "Processando" in msg or "Enviando" in msg or "concluida" in msg:
            color = Fore.YELLOW
            
   
        elif "Saturado" in msg or "cheia" in msg or "AVISO" in msg or "Encerrado" in msg or "ignorad" in msg or "inativo" in msg:
            color = Style.BRIGHT + Fore.RED
            
        
        elif "fila=" in msg or "pendentes=" in msg or "Status" in msg or "Eleição:" in msg or "Eleito" in msg:
            color = Fore.LIGHTBLACK_EX
            
       
        elif "Heartbeat" in msg or "Conectando" in msg or "Conectado" in msg or "Apresentado" in msg or "apresentou" in msg or "Iniciando" in msg or "ativo" in msg or "alvo atualizado" in msg or "registrado" in msg:
            color = Fore.LIGHTBLUE_EX
            
      
        elif record.name == "MASTER":
            color = Fore.CYAN
        elif record.name == "WORKER":
            color = Fore.MAGENTA
            
        
        time_str = self.formatTime(record, "%H:%M:%S")
        
       
        prefix = ""
        if record.name == "MASTER":
            prefix = f"{Fore.BLUE}[MASTER]{Style.RESET_ALL} "
        
        return f"{Fore.LIGHTBLACK_EX}[{time_str}]{Style.RESET_ALL} {prefix}{color}{msg}{Style.RESET_ALL}"


def setup_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    
    if not logger.handlers:
     
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColoredFormatter())
        logger.addHandler(ch)
        
       
        
    return logger

master_logger = setup_logger("MASTER")
worker_logger = setup_logger("WORKER")
