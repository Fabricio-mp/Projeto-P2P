# Master (`master.py`)

## Responsabilidades

- Aceitar conexões TCP de Workers (próprios e emprestados)
- Gerar tarefas periodicamente e enfileirá-las
- Distribuir tarefas da fila para Workers disponíveis
- Confirmar conclusão de tarefas com ACK
- Detectar saturação e negociar ajuda com Masters vizinhos
- Emprestar Workers para Masters vizinhos sobrecarregados

---

## Estado Global

| Variável | Tipo | Descrição |
|---|---|---|
| `workers` | `dict` | Mapa `{ worker_uuid: socket }` dos Workers conectados |
| `pending` | `int` | Contador de tarefas ainda não concluídas |
| `pending_lock` | `Lock` | Mutex para acesso seguro ao contador `pending` |
| `task_queue` | `list` | Fila de tarefas aguardando um Worker disponível |
| `task_queue_lock` | `Lock` | Mutex para acesso seguro à fila |

---

## Funções

### Comunicação

#### `send(sock, payload)`
Serializa `payload` como JSON e envia pela socket com `\n` como terminador. Erros de envio são silenciados (`OSError` ignorado).

#### `receive(sock)`
Lê bytes da socket até encontrar `\n`, então desserializa e retorna o objeto JSON. Retorna `None` em caso de erro ou conexão encerrada.

---

### Validação de Mensagens

#### `valid_heartbeat(msg)`
Valida mensagens de heartbeat e apresentação. Aceita dois formatos:
- `{ TASK: "HEARTBEAT", SERVER_UUID: "<não vazio>" }` — heartbeat de Worker
- `{ WORKER: "ALIVE", WORKER_UUID: "<não vazio>" }` — apresentação de Worker

#### `valid_status_report(msg)`
Valida mensagens de conclusão de tarefa. Campos obrigatórios: `STATUS` (`"OK"` ou `"NOK"`), `TASK` (`"QUERY"`), `WORKER_UUID` (string não vazia).

---

### Fila de Tarefas

#### `enqueue_task(task_id, user, force_nok=False)`
Adiciona uma tarefa à fila com os campos `TASK_ID`, `USER` e `FORCE_NOK`.

#### `dequeue_task()`
Remove e retorna o primeiro item da fila (FIFO). Retorna `None` se a fila estiver vazia.

#### `dispatch_next_task(conn, worker_uuid)`
Tenta retirar uma tarefa da fila e enviá-la ao Worker via `conn`. Se não houver tarefa, envia `{ TASK: "NO_TASK" }`.

---

### Gerenciamento de Workers

#### `handle_worker(worker_uuid, conn, first_msg=None)`
Loop principal de atendimento de um Worker. Processa as mensagens recebidas:

| Mensagem recebida | Ação do Master |
|---|---|
| `WORKER: "ALIVE"` | Registra Worker, despacha próxima tarefa |
| `TASK: "HEARTBEAT"` | Responde ALIVE, despacha próxima tarefa |
| `STATUS: "OK"/"NOK"` | Decrementa `pending`, envia ACK |
| `TASK: "task_done"` | Decrementa `pending` (formato legado) |
| `TASK: "register_worker"` | Registra Worker próprio |
| `TASK: "register_temporary_worker"` | Registra Worker emprestado |
| Mensagem inválida ou `None` | Remove Worker e fecha conexão |

---

### Loop de Aceitação

#### `accept_loop()`
Fica em escuta na porta `MASTER_PORT`. Para cada conexão recebida, lê a primeira mensagem e decide o tratamento:

| Tipo de conexão | Ação |
|---|---|
| Heartbeat ou apresentação de Worker | Inicia thread `handle_worker` |
| Registro de Worker | Inicia thread `handle_worker` |
| `request_help` de Master vizinho | Chama `handle_help_request` |
| `command_release` de Master vizinho | Fecha conexão (Worker retorna ao origin) |

---

### Geração de Carga

#### `load_generator()`
Loop infinito que a cada `REQUEST_INTERVAL` segundos:
1. Gera um `TASK_ID` sequencial (`TASK-0000`, `TASK-0001`, …)
2. Escole um usuário ciclicamente entre `["User1", "User2", "User3", "User4"]`
3. Define `force_nok=True` a cada 5 tarefas (para simular falhas)
4. Chama `enqueue_task` e incrementa `pending`
5. Se `pending > LOAD_THRESHOLD`, dispara thread `ask_for_help`

---

### Negociação entre Masters

#### `ask_for_help()`
Itera sobre `NEIGHBOR_MASTERS` e tenta conectar em cada um. Envia `{ TASK: "request_help", MASTER_PORT: ... }`. Se o vizinho responder com `response_accepted`, encerra a busca.

#### `handle_help_request(conn, msg)`
Responde a um pedido de ajuda de outro Master:
- Se houver mais de 1 Worker conectado: empresta 1 Worker, envia `response_accepted` e redireciona o Worker com `command_redirect`
- Se não houver Workers disponíveis: envia `response_rejected`

---

### Utilitários

#### `build_alive_response()`
Retorna o payload padrão de resposta a heartbeat: `{ SERVER_UUID, TASK: "HEARTBEAT", RESPONSE: "ALIVE" }`.

#### `borrowed_worker(msg)`
Retorna `True` se o `SERVER_UUID` da mensagem for diferente do UUID do Master atual (indica Worker emprestado de outro Master).

---

## Fluxo de Mensagens JSON

### Apresentação de Worker
```json
// Worker → Master
{ "WORKER": "ALIVE", "WORKER_UUID": "abc-123" }

// Master → Worker (com tarefa)
{ "TASK": "QUERY", "USER": "User1", "TASK_ID": "TASK-0000", "FORCE_NOK": false, "SERVER_UUID": "xyz-456" }

// Master → Worker (sem tarefa)
{ "TASK": "NO_TASK", "SERVER_UUID": "xyz-456" }
```

### Conclusão de Tarefa
```json
// Worker → Master
{ "STATUS": "OK", "TASK": "QUERY", "WORKER_UUID": "abc-123", "TASK_ID": "TASK-0000" }

// Master → Worker
{ "STATUS": "ACK", "WORKER_UUID": "abc-123", "TASK_ID": "TASK-0000" }
```

### Heartbeat
```json
// Worker → Master
{ "TASK": "HEARTBEAT", "SERVER_UUID": "abc-123" }

// Master → Worker
{ "TASK": "HEARTBEAT", "RESPONSE": "ALIVE", "SERVER_UUID": "xyz-456" }
```

### Pedido de Ajuda entre Masters
```json
// Master A → Master B
{ "SERVER_UUID": "aaa", "TASK": "request_help", "MASTER_PORT": 5000 }

// Master B → Master A (aceitou)
{ "SERVER_UUID": "bbb", "TASK": "response_accepted", "WORKERS_TO_SEND": 1 }

// Master B → Worker (redirecionamento)
{ "TASK": "command_redirect", "NEW_MASTER_HOST": "192.168.1.10", "NEW_MASTER_PORT": 5000 }

// Master B → Master A (rejeitou)
{ "SERVER_UUID": "bbb", "TASK": "response_rejected" }
```

---

## Threads em Execução

| Thread | Função | Tipo |
|---|---|---|
| Principal | `load_generator()` | Foreground |
| Aceitação | `accept_loop()` | Daemon |
| Por Worker | `handle_worker()` | Daemon |
| Ajuda | `ask_for_help()` | Daemon |

---

## Modos de Operação

### Sprint 1 (`SPRINT1_HEARTBEAT_ONLY = True`)
Apenas `accept_loop` é iniciado. Mensagens `request_help` e `command_release` são ignoradas. Nenhuma tarefa é gerada.

### Sprint 2 (`SPRINT1_HEARTBEAT_ONLY = False`)
Fluxo completo: `accept_loop` em thread daemon + `load_generator` no processo principal.
