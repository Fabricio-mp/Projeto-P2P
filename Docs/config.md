# Configuração do Sistema (`config.py`)

## Visão Geral

O arquivo `config.py` centraliza todos os parâmetros do sistema. Ele é importado tanto pelo `master.py` quanto pelo `worker.py`. Um UUID único de servidor é gerado a cada execução.

---

## Variáveis

### Identidade

| Variável | Tipo | Descrição |
|---|---|---|
| `SERVER_UUID` | `str` (UUID v4) | Identificador único gerado automaticamente a cada execução do processo |

---

### Rede do Master

| Variável | Tipo | Padrão | Descrição |
|---|---|---|---|
| `MASTER_HOST` | `str` | `"10.62.206.22"` | Endereço em que o Master fará `bind`. Use `"0.0.0.0"` para aceitar conexões em todas as interfaces locais |
| `MASTER_PORT` | `int` | `5000` | Porta TCP principal de comunicação Master ↔ Worker e Master ↔ Master |

---

### Comportamento de Tarefas

| Variável | Tipo | Padrão | Descrição |
|---|---|---|---|
| `LOAD_THRESHOLD` | `int` | `5` | Número máximo de tarefas pendentes antes de o Master considerar-se saturado e pedir ajuda a um vizinho |
| `TASK_DURATION` | `int` | `3` | Duração em segundos que o Worker simula para processar cada tarefa (`time.sleep`) |
| `REQUEST_INTERVAL` | `float` | `1.0` | Intervalo em segundos entre geração de novas tarefas pelo `load_generator` do Master |

---

### Heartbeat

| Variável | Tipo | Padrão | Descrição |
|---|---|---|---|
| `HEARTBEAT_INTERVAL` | `float` | `30.0` | Intervalo em segundos entre cada envio de heartbeat pelo Worker ao Master |
| `HEARTBEAT_TIMEOUT` | `float` | `5.0` | Tempo máximo em segundos que o Worker aguarda resposta do Master antes de considerar a conexão perdida |

---

### Controle de Sprint

| Variável | Tipo | Padrão | Descrição |
|---|---|---|---|
| `SPRINT1_HEARTBEAT_ONLY` | `bool` | `False` | Quando `True`, o Master opera apenas com heartbeat (modo Sprint 1). Quando `False`, o fluxo completo de fila e processamento está ativo (Sprint 2) |

---

### Reconexão e Eleição

| Variável | Tipo | Padrão | Descrição |
|---|---|---|---|
| `CONNECTION_ERROR_THRESHOLD` | `int` | `4` | Número de falhas consecutivas de conexão antes de o Worker iniciar uma eleição de novo Master |
| `ELECTION_PORT` | `int` | `5100` | Porta TCP usada exclusivamente para comunicação de eleição entre Workers |
| `ELECTION_RETRY_INTERVAL` | `float` | `2.0` | Intervalo em segundos entre tentativas de reconexão após falha |
| `ELECTION_CANDIDATES` | `list[str]` | `["10.62.206.22"]` | Lista de IPs candidatos a Master na eleição. O Worker com maior espaço livre em disco vence |

---

### Masters Vizinhos

| Variável | Tipo | Padrão | Descrição |
|---|---|---|---|
| `NEIGHBOR_MASTERS` | `list[tuple]` | `[("10.62.206.22", 5000)]` | Lista de Masters vizinhos `(host, porta)` para os quais o Master local pode pedir ajuda quando saturado |

---

## Exemplos de Configuração

### Ambiente local (desenvolvimento)
```python
MASTER_HOST = "0.0.0.0"
MASTER_PORT = 5000

ELECTION_CANDIDATES = ["127.0.0.1"]
NEIGHBOR_MASTERS = [("127.0.0.1", 5000)]
```

### Ambiente em rede com dois Masters
```python
# Na máquina A (192.168.1.10)
MASTER_HOST = "0.0.0.0"
NEIGHBOR_MASTERS = [("192.168.1.20", 5000)]
ELECTION_CANDIDATES = ["192.168.1.10", "192.168.1.20"]

# Na máquina B (192.168.1.20)
MASTER_HOST = "0.0.0.0"
NEIGHBOR_MASTERS = [("192.168.1.10", 5000)]
ELECTION_CANDIDATES = ["192.168.1.10", "192.168.1.20"]
```

---

## Observações

- O `print('oi')` no final do arquivo é executado toda vez que qualquer módulo importa o `config.py`. Pode ser removido sem impacto no sistema.
- `SERVER_UUID` é regenerado a cada vez que o processo sobe — não é persistido entre execuções.
