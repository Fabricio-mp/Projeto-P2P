# Implementação da Sprint 03 - P2P e Balanceamento de Carga Dinâmico

O objetivo desta sprint é implementar o protocolo P2P completo para negociação e transferência de Workers entre Masters, permitindo o balanceamento dinâmico de carga.

## User Review Required

> [!IMPORTANT]
> **Formato de Mensagens Master-Worker:**
> A especificação define que "Todas as mensagens entre Masters devem seguir o formato `{"type": "...", "request_id": "...", "payload": {...}}`".
> Para as mensagens envolvendo o Worker (`command_redirect`, `register_temporary_worker` e `command_release`), manteremos a compatibilidade com a estrutura de dicionário simples do Worker (ex: adicionando as chaves diretamente no JSON, como `{"TASK": "command_redirect", "NEW_MASTER_ADDRESS": "..."}`), ou você prefere que **todas** as novas comunicações Master-Worker também adotem o wrapper P2P `{type, request_id, payload}`? No plano abaixo, assumi que adotaremos a nova estrutura para os novos comandos.

> [!IMPORTANT]
> **Timeout e Thresholds:**
> - Qual deve ser o limiar inferior (histerese) para devolver os workers? Assumi `LOAD_THRESHOLD * 0.5`.
> - Os workers emprestados devem continuar respondendo aos `HEARTBEAT`s de forma padrão? Assumi que sim.

## Open Questions

1. Em caso de falha de conexão do Master A com o Master B para envio do `notify_worker_returned`, o sistema deve tentar novamente ou o Master B apenas lidará com o retorno do Worker de forma passiva? (Assumi envio *fire-and-forget* para não bloquear o Master A).

## Proposed Changes

### Master (`master.py`)

A lógica de P2P será isolada para não misturar com o tratamento de Workers tradicionais.

- **Parsing de Mensagens:**
  - Adição de um parser case-sensitive que exige os campos `type`, `request_id`, e `payload` para comunicação Master-to-Master. Campos desconhecidos serão ignorados.
  - Se a mensagem vier sem o campo `type`, o sistema fará fallback para as regras antigas (para retrocompatibilidade com workers antigos).

- **Negociação P2P (`request_help` / `response_accepted`):**
  - A função `ask_for_help` construirá o envelope JSON padrão. O `payload` incluirá `master_id`, `current_load`, `capacity`, e `workers_needed`.
  - A função `handle_help_request` responderá `response_accepted` (se tiver workers disponíveis e load baixo) ou `response_rejected`.

- **Redirecionamento de Workers (`command_redirect`):**
  - Ao aceitar o pedido, o Master B enviará `command_redirect` ao(s) worker(s) escolhido(s).

- **Devolução e Histerese (`command_release` e `notify_worker_returned`):**
  - Monitoramento contínuo da fila (no `load_generator` ou thread separada).
  - Quando `pending < LOAD_THRESHOLD * 0.5`, o Master A envia `command_release` para workers emprestados.
  - Logo em seguida, envia `notify_worker_returned` ao Master B.

### Worker (`worker.py`)

- **Redirecionamento:**
  - O loop principal processará o `command_redirect` do Master B e reajustará o `master_target`.
  - No `register_with_master`, o Worker detectará que seu alvo atual é diferente do original e enviará a nova estrutura `{"type": "register_temporary_worker", "payload": {"worker_id": "...", "original_master_address": "..."}}`.
  
- **Resiliência e Retorno:**
  - Se o Worker desconectar do Master A, ele limpa o alvo temporário e retorna para o Master original (Master B).
  - Ao receber `command_release` do Master A, ele fará o mesmo (voltará ao Master B).

## Verification Plan

### Automated / Manual Tests
1. Subir 2 Masters (A e B) em portas diferentes (ex: 5000 e 5001).
2. Subir 1 Worker apontando para o Master B.
3. Saturar o Master A gerando tarefas massivas (`load_generator` disparando rápido).
4. **Validar:**
   - O Master A manda `request_help` para o Master B.
   - O Master B devolve `response_accepted` e manda `command_redirect` para seu Worker.
   - O Worker reconecta no Master A e envia `register_temporary_worker`.
   - O log do Worker e Master reflete a operação com as cores configuradas.
5. Quando o Master A esvaziar sua fila:
   - Validar se o Master A emite `command_release` para o Worker.
   - Validar envio de `notify_worker_returned` para o Master B.
   - Validar que o Worker voltou a conectar no Master B.
