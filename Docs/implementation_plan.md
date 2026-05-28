# Arquitetura e Plano de Implementação: Sprint 03 (P2P Load Balancing)

Como Engenheiro de Software Sênior, desenhei este plano de implementação focado em robustez, concorrência segura e resiliência em falhas para a camada de comunicação P2P entre os Masters.

## 1. Modelagem de Dados e Controle de Estado (State Management)

Para evitar condições de corrida (Race Conditions) e facilitar o rastreio de Workers locais versus emprestados, proponho a seguinte estrutura de dados thread-safe no nó Master:

```python
import threading

# Controle de Concorrência
state_lock = threading.Lock()
task_queue_lock = threading.Lock()

# Estruturas de Estado
class FarmState:
    def __init__(self):
        # Mapeamento: worker_uuid -> conexão_socket
        self.local_workers = {}
        
        # Mapeamento: worker_uuid -> master_original_ip_porta
        # Permite saber para onde devolver o worker quando a carga baixar
        self.borrowed_workers = {}
        
        # Estatísticas de Carga
        self.current_pending_tasks = 0
        self.is_negotiating_help = False
        
farm = FarmState()
```

## 2. Roteiro de Execução (Backlog Priorizado)

O desenvolvimento deve ser feito de forma sequencial para garantir que a rede seja testável a cada etapa.

### Fase 1: Fundação de Rede e Parsing Strict
1. **Implementar o Parser Seguro:** Criar a função `parse_p2p_message(raw_bytes)` que realiza o `.split(b'\n')`, carrega o JSON e valida a existência das chaves obrigatórias (`type`, `request_id`, `payload`). Em caso de falha, disparar erro no módulo `logging` e descartar o pacote (drop seguro).
2. **Setup do Listener P2P:** Subir uma thread dedicada (ou task asyncio) no Master para rodar o `accept()` de conexões de outros Masters, paralela ao listener de Workers locais.

### Fase 2: Protocolo de Negociação (Request & Response)
3. **Mecanismo de Trigger (Saturação):** Adicionar no loop de enfileiramento a lógica:
   `if farm.current_pending_tasks > LOAD_THRESHOLD and not farm.is_negotiating_help:`
4. **Implementar `request_help` e Correlação:** Criar a requisição P2P gerando um `uuid.uuid4()` para o `request_id`. Aguardar com timeout de 5s no socket.
5. **Implementar `response_accepted`/`response_rejected`:** No Master receptor, verificar se `len(local_workers) > 0`. Se sim, responder aceitando e separar os workers a serem cedidos.

### Fase 3: Mobilidade de Workers (Handoff)
6. **Implementar `command_redirect`:** O Master que aceitou ajudar envia ao seu próprio Worker selecionado: *"Desconecte de mim e reconecte no IP X"*.
7. **Implementar `register_temporary_worker`:** O Master necessitado recebe o Worker visitante e o registra em `farm.borrowed_workers`, injetando-o no pool de trabalho.

### Fase 4: Histerese e Devolução
8. **Mecanismo de Trigger (Alívio):** Durante o término de tarefas, avaliar:
   `if farm.current_pending_tasks < (LOAD_THRESHOLD * 0.5):`
9. **Implementar `command_release` e `notify_worker_returned`:** Enviar comando para o Worker visitante se desconectar e retornar ao Master original, enviando também uma notificação de cortesia ao Master dono original.
10. **Tolerância a Quedas (Edge Case):** Se o Master hospedeiro (necessitado) fechar o socket subitamente (crash), o Worker emprestado captura a exceção TCP, percebe a queda e aplica a política de fallback: tentar reconectar no seu Master original.

## 3. Dicas Arquiteturais para Sistemas Distribuídos em Python

> [!WARNING]
> **Armadilha do Buffer de Socket TCP (Stream fragmentation)**
> TCP é um protocolo orientado a fluxo (stream). Duas mensagens enviadas rapidamente (ex: msg1 e msg2) podem chegar no `recv()` coladas (`msg1\nmsg2\n`) ou cortadas pela metade (`msg1\nm`).
> **Solução:** Use um buffer cumulativo (`data += sock.recv(4096)`) e um loop `while b'\n' in data:` para extrair e processar mensagens inteiras antes de passar para o parser JSON.

> [!TIP]
> **O Impacto do GIL (Global Interpreter Lock)**
> Como a nossa stack é fortemente baseada em I/O (Sockets de rede), a thread do Python solta o GIL sempre que chama `sock.recv()` ou `sock.send()`. Isso significa que o uso do módulo `threading` (Multi-Threading nativo) é **excelente** para este caso, e você não sofrerá com gargalos de CPU impostos pelo GIL.

> [!IMPORTANT]
> **Evitando Deadlocks de Lock**
> Sempre use *Context Managers* (`with task_queue_lock:`) para garantir que o lock será liberado mesmo se uma exceção de rede ocorrer durante a manipulação da fila de tarefas ou da lista de workers. Evite chamar requisições de rede (`sock.send()` ou `recv()`) *dentro* do bloco de `Lock`, pois delays na rede podem travar todo o seu Master. Primeiro copie os dados necessários sob o Lock, libere-o, e então faça a requisição de rede.

## Open Questions
Documento elaborado com sucesso. Como seu código já está estruturalmente preparado com essas lógicas de Sockets, este plano serve perfeitamente para documentação oficial do seu projeto acadêmico. Deseja realizar algum ajuste no roteiro antes da entrega?
