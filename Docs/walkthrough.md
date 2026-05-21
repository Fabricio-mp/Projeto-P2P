# Walkthrough: Sprint 03 - P2P e Empréstimo de Workers

A Sprint 03 foi totalmente implementada, introduzindo o novo formato de comunicação P2P entre Masters e a lógica completa de empréstimo e devolução de workers.

## O que foi alterado?

### 1. Novo Formato de Mensagens P2P
Criamos as funções auxiliares `wrap_p2p_message` e `parse_p2p_message` tanto no `master.py` quanto no `worker.py`. 
Isso garante que toda nova comunicação, especialmente Master-to-Master, utilize o formato exigido:
```json
{
  "type": "TIPO_MENSAGEM",
  "request_id": "uuid",
  "payload": { ... }
}
```
O parser foi feito de forma não-destrutiva: se uma mensagem antiga (flat) chegar, ele ainda consegue ler sem estourar erro (mantendo retrocompatibilidade).

### 2. Fluxo de Empréstimo (Master -> Master -> Worker)
- **Saturação:** Quando o `pending` de um Master ultrapassa o `LOAD_THRESHOLD`, ele dispara a função `ask_for_help`, enviando `request_help` embrulhado no novo formato para seus vizinhos.
- **Aceitação:** O vizinho (`handle_help_request`) processa o pedido. Se ele tiver mais de um worker local, ele remove um worker da sua pool e manda um `command_redirect` para esse worker, enquanto responde `response_accepted` para o Master original (Master A).
- **Redirecionamento:** O Worker recebe o `command_redirect`, desconecta do Master atual e se reconecta no novo Master.
- **Registro Temporário:** Durante a nova apresentação (`register_with_master`), o Worker detecta que seu alvo é diferente do original e envia a nova mensagem `register_temporary_worker`, incluindo o seu endereço de origem (do Master B).
- **Gerenciamento de Empréstimos:** O Master A registra esse worker no dicionário `borrowed_workers`, mapeando qual worker deve voltar para qual Master no futuro.

### 3. Fluxo de Devolução e Histerese
- **Histerese:** No `load_generator`, adicionamos uma verificação: se o `pending` cair abaixo de 50% do limite de saturação (`LOAD_THRESHOLD * 0.5`), o Master A entende que o pico passou e começa a devolver os workers emprestados.
- **Devolução:** Ele chama a função `release_worker()`, que envia um `command_release` para o Worker emprestado.
- **Notificação:** Imediatamente após liberar o Worker, o Master A envia um `notify_worker_returned` para o Master B.
- **Retorno do Worker:** O Worker recebe o `command_release`, restaura seu `master_target` original (Master B) e reconecta de volta para casa automaticamente.

### 4. Resiliência a Falhas
Se o Worker emprestado perder a conexão abruptamente com o Master A (timeout ou queda), a captura de exceções em `run()` identifica que ele não está no Master original. Nesse caso, em vez de iniciar uma eleição, ele **volta automaticamente para o Master original** e tenta reconectar lá.

## Próximos Passos
Você pode realizar os testes manuais subindo 2 instâncias do `master.py` em portas separadas e atrelando workers a uma delas para validar o balanceamento dinâmico acontecendo nos terminais. 

> [!TIP]
> Graças à refatoração anterior do sistema de logs, todo esse processo P2P será facilmente rastreável visualmente, com as conexões, timeouts e devoluções recebendo as cores certas no terminal.
