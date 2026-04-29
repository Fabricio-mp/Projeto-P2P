# Visão Geral do Sistema P2P

## O que é

Sistema distribuído de processamento de tarefas baseado no modelo **Master/Worker**. Um nó Master centraliza a geração e distribuição de tarefas; múltiplos Workers conectam-se a ele, recebem tarefas, processam e reportam o resultado. O sistema suporta múltiplos Masters em rede, eleição automática de novo Master em caso de falha, e empréstimo de Workers entre Masters sobrecarregados.

---

## Arquitetura

```
┌─────────────────────────────────────────────────────┐
│                   MASTER (master.py)                │
│                                                     │
│  ┌─────────────┐     ┌──────────────────────────┐  │
│  │ load_genera-│────▶│      task_queue          │  │
│  │ tor()       │     │  [TASK-0000, TASK-0001…] │  │
│  └─────────────┘     └────────────┬─────────────┘  │
│                                   │ dispatch        │
│                      ┌────────────▼─────────────┐  │
│                      │      accept_loop()        │  │
│                      │  (escuta porta 5000)      │  │
│                      └────────────┬─────────────┘  │
└───────────────────────────────────┼─────────────────┘
                                    │ TCP
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
   ┌──────────▼──────┐   ┌──────────▼──────┐   ┌─────────▼───────┐
   │  Worker A       │   │  Worker B       │   │  Worker C       │
   │  (worker.py)    │   │  (worker.py)    │   │  (worker.py)    │
   └─────────────────┘   └─────────────────┘   └─────────────────┘

Masters vizinhos comunicam-se entre si via TCP (porta 5000)
Workers comunicam-se entre si via porta de eleição (5100)
```

---

## Fluxo de Comunicação

### 1. Apresentação do Worker
```
Worker ──▶ Master : { WORKER: "ALIVE", WORKER_UUID: "..." }
Master ──▶ Worker : { TASK: "QUERY", TASK_ID: "...", USER: "..." }
           ou
Master ──▶ Worker : { TASK: "NO_TASK" }
```

### 2. Processamento de Tarefa
```
Worker processa (sleep TASK_DURATION segundos)
Worker ──▶ Master : { STATUS: "OK"/"NOK", TASK: "QUERY", TASK_ID: "..." }
Master ──▶ Worker : { STATUS: "ACK", TASK_ID: "..." }
```

### 3. Heartbeat periódico
```
Worker ──▶ Master : { TASK: "HEARTBEAT", SERVER_UUID: "..." }
Master ──▶ Worker : { TASK: "HEARTBEAT", RESPONSE: "ALIVE" }
Master ──▶ Worker : (próxima tarefa da fila, se houver)
```

### 4. Saturação e ajuda entre Masters
```
Master A ──▶ Master B : { TASK: "request_help", MASTER_PORT: 5000 }
Master B ──▶ Master A : { TASK: "response_accepted" }
Master B ──▶ Worker X : { TASK: "command_redirect", NEW_MASTER_HOST: "...", NEW_MASTER_PORT: 5000 }
```

### 5. Eleição de novo Master
```
Worker ──▶ Candidatos : { TASK: "ELECTION_QUERY" }
Candidatos ──▶ Worker : { TASK: "ELECTION_RESPONSE", FREE_BYTES: ... }
Worker ──▶ Todos      : { TASK: "ELECTION_ANNOUNCE", NEW_MASTER_HOST: "..." }
Todos  ──▶ Worker     : { TASK: "ACK" }
```

---

## Como Rodar

### Pré-requisitos
- Python 3.x
- Arquivos `config.py`, `master.py` e `worker.py` na mesma pasta

### Configuração mínima (local)
Edite o `config.py`:
```python
MASTER_HOST = "0.0.0.0"
ELECTION_CANDIDATES = ["127.0.0.1"]
NEIGHBOR_MASTERS = [("127.0.0.1", 5000)]
```

### Iniciar o Master
```bash
python master.py
```

### Iniciar Worker(s)
```bash
# Em terminais separados
python worker.py 127.0.0.1 5000
python worker.py 127.0.0.1 5000
```

---

## Componentes

| Arquivo | Papel |
|---|---|
| `config.py` | Configurações centrais do sistema |
| `master.py` | Servidor: gera tarefas, distribui para workers, negocia com vizinhos |
| `worker.py` | Cliente: recebe tarefas, processa, reporta, elege novo master se necessário |

---

## Portas Utilizadas

| Porta | Uso |
|---|---|
| `5000` | Comunicação Master ↔ Worker e Master ↔ Master |
| `5100` | Eleição de novo Master entre Workers |
