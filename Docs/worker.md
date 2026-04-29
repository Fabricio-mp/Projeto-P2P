# Worker (`worker.py`)

## Responsabilidades

- Conectar a um Master e manter a conexão ativa via heartbeat
- Receber tarefas do Master e processá-las (simulação com `sleep`)
- Reportar o resultado de cada tarefa e aguardar ACK
- Detectar falha do Master e iniciar eleição de novo Master
- Participar de eleições como candidato e como votante
- Ser redirecionado para outro Master quando emprestado

---

## Estado Global

| Variável | Tipo | Descrição |
|---|---|---|
| `WORKER_UUID` | `str` | UUID v4 gerado uma vez na inicialização do processo |
| `master_target` | `dict` | `{ host, port }` do Master alvo atual |
| `master_target_lock` | `Lock` | Mutex para acesso seguro ao `master_target` |
| `original_master_target` | `tuple` | `(host, port)` do primeiro Master ao qual o Worker se conectou |
| `original_master_uuid` | `str` | UUID do primeiro Master |
| `current_master_uuid` | `str` | UUID do Master atualmente conectado |
| `last_registration_master_uuid` | `str` | UUID do Master da última apresentação bem-sucedida |
| `master_process` | `Popen` | Processo do `master.py` local, se este Worker tiver sido eleito Master |
| `master_process_lock` | `Lock` | Mutex para acesso seguro ao `master_process` |

---

## Funções

### Comunicação

#### `send(sock, payload)`
Serializa `payload` como JSON e envia pela socket com `\n` como terminador.

#### `receive(sock)`
Lê bytes até encontrar `\n` e retorna o objeto JSON desserializado. Retorna `None` em erro ou desconexão.

#### `receive_with_timeout(sock, timeout_seconds)`
Variante de `receive` que aplica um timeout temporário à socket e restaura o timeout original ao final.

---

### Conexão

#### `connect(host, port)`
Cria uma socket TCP com timeout `HEARTBEAT_TIMEOUT` e conecta ao endereço informado.

#### `local_addresses()`
Retorna o conjunto de endereços locais da máquina (`127.0.0.1`, `localhost`, IPs da interface de rede).

#### `is_local_host(host)`
Retorna `True` se `host` pertencer aos endereços locais da máquina.

---

### Apresentação e Registro

#### `build_presentation_payload()`
Monta o payload de apresentação do Worker:
```json
{ "WORKER": "ALIVE", "WORKER_UUID": "..." }
```
Se o Worker estiver emprestado (Master atual diferente do original), inclui também `SERVER_UUID` do Master original.

#### `register_with_master(sock)`
Envia o payload de apresentação, recebe a resposta do Master e:
- Atualiza `current_master_uuid` e `original_master_uuid`
- Se a resposta for uma tarefa (`QUERY`), chama `process_task` imediatamente
- Se for `NO_TASK`, registra no log
- Retorna a resposta recebida ou `None` em falha

---

### Processamento de Tarefas

#### `process_task(sock, task_msg)`
Processa uma tarefa recebida do Master:
1. Extrai `TASK_ID`, `USER` e `FORCE_NOK`
2. Aguarda `TASK_DURATION` segundos (simulação)
3. Envia status `OK` ou `NOK` (conforme `FORCE_NOK`)
4. Aguarda ACK do Master

#### `handle_master_message(sock, msg)`
Despacha mensagens recebidas do Master fora do fluxo de apresentação:

| Mensagem | Ação |
|---|---|
| `TASK: "QUERY"` | Chama `process_task` |
| `TASK: "NO_TASK"` | Registra no log |
| `TASK: "HEARTBEAT", RESPONSE: "ALIVE"` | Confirma heartbeat no log |

---

### Gerenciamento do Master Alvo

#### `set_master_target(host, port, reason="")`
Atualiza o Master alvo. Se for o primeiro Master configurado, salva também em `original_master_target`.

#### `get_master_target()`
Retorna `(host, port)` do Master alvo atual de forma thread-safe.

---

### Eleição de Master

#### `election_server()`
Servidor TCP que escuta na porta `ELECTION_PORT` e responde a mensagens de eleição em threads separadas.

#### `handle_election_message(conn)`
Processa uma mensagem de eleição:

| Mensagem recebida | Ação |
|---|---|
| `ELECTION_QUERY` | Responde com `FREE_BYTES` do disco local |
| `ELECTION_ANNOUNCE` | Atualiza Master alvo; sobe `master.py` local se eleito |

#### `query_candidate_disk(host)`
Consulta o espaço livre em disco de um candidato via `ELECTION_QUERY`. Para hosts locais, retorna o valor diretamente sem conexão de rede.

#### `announce_winner(host, winner_host, winner_port)`
Envia `ELECTION_ANNOUNCE` para um host com o resultado da eleição. Para hosts locais, atualiza diretamente.

#### `run_master_election()`
Executa o processo completo de eleição:
1. Consulta espaço livre em disco de todos os candidatos
2. Elege o candidato com maior espaço livre (desempate pelo IP)
3. Anuncia o vencedor para todos os candidatos
4. Verifica se o consenso foi atingido (maioria simples)
5. Atualiza o Master alvo e sobe `master.py` local se necessário

#### `unique_candidates()`
Retorna lista deduplicada de candidatos: `ELECTION_CANDIDATES` + endereços locais da máquina.

---

### Gerenciamento do Processo Master Local

#### `get_free_disk_bytes()`
Retorna o espaço livre em bytes no diretório do projeto.

#### `ensure_local_master_running()`
Verifica se há um processo `master.py` local em execução. Se não houver, inicia um via `subprocess.Popen`.

---

## Loop Principal

### `run(host, port)`
Loop de execução do Worker:

```
1. Configura master_target inicial
2. Inicia election_server em thread daemon
3. Loop infinito:
   a. Conecta ao master_target atual
   b. Chama register_with_master (apresentação + primeira tarefa)
   c. Aguarda HEARTBEAT_INTERVAL segundos
   d. Em caso de erro:
      - Incrementa consecutive_errors
      - Se >= CONNECTION_ERROR_THRESHOLD: inicia eleição
      - Aguarda ELECTION_RETRY_INTERVAL e tenta novamente
```

---

## Fluxo de Mensagens JSON

### Apresentação
```json
// Worker → Master
{ "WORKER": "ALIVE", "WORKER_UUID": "abc-123" }

// Worker emprestado → Master (inclui UUID do Master original)
{ "WORKER": "ALIVE", "WORKER_UUID": "abc-123", "SERVER_UUID": "original-master-uuid" }
```

### Status de Tarefa
```json
// Worker → Master
{ "STATUS": "OK", "TASK": "QUERY", "WORKER_UUID": "abc-123", "TASK_ID": "TASK-0000" }

// Master → Worker
{ "STATUS": "ACK", "WORKER_UUID": "abc-123", "TASK_ID": "TASK-0000" }
```

### Eleição
```json
// Iniciador → Candidato
{ "TASK": "ELECTION_QUERY", "WORKER_UUID": "abc-123" }

// Candidato → Iniciador
{ "TASK": "ELECTION_RESPONSE", "WORKER_UUID": "abc-123", "FREE_BYTES": 126760005632 }

// Vencedor → Todos
{ "TASK": "ELECTION_ANNOUNCE", "NEW_MASTER_HOST": "127.0.0.1", "NEW_MASTER_PORT": 5000, "INITIATOR_UUID": "abc-123" }

// Confirmação
{ "TASK": "ACK", "WORKER_UUID": "abc-123" }
```

### Redirecionamento
```json
// Master B → Worker (quando emprestado)
{ "TASK": "command_redirect", "NEW_MASTER_HOST": "192.168.1.10", "NEW_MASTER_PORT": 5000 }
```

---

## Ciclo de Vida da Conexão

```
[INÍCIO]
    │
    ▼
Conecta ao Master
    │
    ▼
Envia apresentação (WORKER: ALIVE)
    │
    ├─▶ Recebe QUERY ──▶ Processa tarefa ──▶ Envia STATUS ──▶ Recebe ACK
    │
    └─▶ Recebe NO_TASK
    │
    ▼
Aguarda HEARTBEAT_INTERVAL
    │
    ▼
Reconecta (register_with_master novamente)
    │
    ▼ (em caso de erro)
consecutive_errors++
    │
    ├─▶ < CONNECTION_ERROR_THRESHOLD: aguarda e tenta novamente
    │
    └─▶ >= CONNECTION_ERROR_THRESHOLD: inicia ELEIÇÃO
              │
              ▼
         Novo Master eleito ──▶ Reconecta ao novo Master
```

---

## Threads em Execução

| Thread | Função | Tipo |
|---|---|---|
| Principal | `run()` | Foreground |
| Eleição | `election_server()` | Daemon |
| Por mensagem de eleição | `handle_election_message()` | Daemon |

---

## Critério de Eleição

O Master eleito é o candidato com **maior espaço livre em disco**. Em caso de empate, vence o de maior IP lexicográfico. O consenso é válido quando a maioria simples dos candidatos (`N/2 + 1`) confirma com ACK.
