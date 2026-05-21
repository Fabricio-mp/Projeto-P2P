# Sprint 03: Balanceamento de Carga Dinâmico P2P

Este documento descreve a arquitetura, as lógicas de negócio e as soluções técnicas implementadas durante a **Sprint 3** do projeto. O objetivo principal desta sprint foi estabelecer uma comunicação direta entre nós Masters (P2P) para que pudessem socorrer uns aos outros em momentos de pico de processamento, emprestando Workers dinamicamente.

---

## 1. O Conceito de Saturação
Cada nó Master possui um gerador de carga (tarefas) e uma fila de processamento mantida por seus Workers. No arquivo de configuração (`config.py`), definimos a constante `LOAD_THRESHOLD = 5`.

A lógica de saturação funciona da seguinte forma:
- O Master monitora constantemente sua contagem de tarefas **pendentes**.
- Quando as tarefas pendentes ultrapassam o `LOAD_THRESHOLD`, o Master é considerado **Saturado**.
- Para evitar gargalos e latência, ao invés de simplesmente enfileirar infinitamente, o Master tenta buscar recursos ociosos na rede.

## 2. Pedido de Ajuda P2P (`request_help`)
Assim que o Master A satura, ele consulta sua lista de vizinhos conhecidos (`NEIGHBOR_MASTERS`). Ele abre uma conexão TCP limpa (cliente) para a porta de servidor (TCP) do Master B e envia um JSON padrão de requisição de ajuda:
```json
{
  "type": "request_help",
  "request_id": "uuid_unico",
  "payload": {
    "requester_address": ["127.0.0.1", 5000]
  }
}
```

## 3. Avaliação de Empréstimo (Master B)
O Master B recebe o pedido e toma uma decisão lógica:
> *"Eu posso ajudar o Master A sem me prejudicar?"*

A regra de negócio implementada dita que um Master **só empresta recursos se possuir mais de 1 Worker** (`len(workers) > 1`). 
- **Se não puder:** Ele responde negativamente (`response_rejected`) e o Master A voltará a pedir ajuda alguns segundos depois se continuar saturado.
- **Se puder:** Ele responde afirmativamente (`response_accepted`) e inicia o protocolo de doação.

## 4. O Redirecionamento (`command_redirect`)
Tendo aceitado o pedido, o Master B seleciona um de seus Workers (o primeiro da lista) e envia uma ordem TCP para ele: `command_redirect`.

A grande sacada arquitetural aqui foi lidar com a **concorrência**:
- Se o Worker estiver no meio do processamento de uma tarefa, ele **não aborta**. Ele termina a tarefa em andamento (mantendo a consistência do sistema) e só então processa o redirecionamento.
- O Worker atualiza seu alvo de conexão (`MASTER_HOST` e `MASTER_PORT`) para o endereço do Master A e se desconecta fisicamente do Master B.

## 5. O Worker Temporário
Ao se conectar no Master A, o Worker não se apresenta como um operário comum. Ele envia a mensagem `register_temporary_worker`, que diz ao Master A: *"Estou aqui para ajudar, mas meu verdadeiro dono é o Master B"*.

O Master A armazena esse Worker em um dicionário especial (`borrowed_workers`), e a partir desse momento, passa a enviar as tarefas da sua fila lotada para ele, conseguindo drenar o gargalo mais rapidamente (já que agora possui seus próprios workers + o emprestado operando em paralelo).

## 6. Histerese e Devolução (`command_release`)
O sistema não pode reter recursos emprestados para sempre. Implementamos um mecanismo de **Histerese** (limiares diferentes de ativação e desativação para evitar *flickering* / troca rápida constante).

- A ajuda foi **solicitada** quando a fila passou de `5` (`LOAD_THRESHOLD`).
- A ajuda será **devolvida** apenas quando a fila cair para menos da metade (`<= LOAD_THRESHOLD * 0.5`, ou seja, 2 pendentes ou menos).

Quando a fila chega nesse nível seguro, o Master A envia o `command_release` para o Worker emprestado. O Worker se desconecta do Master A, busca na sua memória quem era o seu Master original (Master B) e reconecta de volta para "casa". O ciclo está concluído.

---

## Desafios Técnicos Solucionados (Para Comentar com o Professor)

Durante a implementação, dois bugs cruciais de sistemas distribuídos foram neutralizados. Comentá-los vai agregar muito valor à apresentação:

### 1. O Problema do Handshake Assíncrono (Deadlock)
Quando o Worker chegava no novo Master se identificando como *temporário*, o Master registrava ele no sistema, mas **esquecia** de enviar a primeira tarefa (o ACK de boas-vindas). Como o Worker esperava uma resposta e o Master esperava que o Worker pedisse uma tarefa, os dois entravam em *deadlock* e o Worker caía por timeout após 5 segundos. 
**Solução:** Inclusão imediata do método de despacho de tarefa na rotina de registro temporário.

### 2. O Clássico Bug de TCP Stream Buffering
Em redes locais, o TCP é extremamente rápido. Quando o Master enviava o "Sinal de Vida (ALIVE)" e, um milissegundo depois, a nova "Tarefa", o protocolo de rede juntava essas duas mensagens em **um único pacote de dados**.
O código antigo lia o pacote, procurava a quebra de linha `\n`, tirava a primeira mensagem (ALIVE) e acidentalmente jogava o restante do pacote (A Tarefa) fora da memória. Isso fazia as tarefas desaparecerem no vácuo.
**Solução:** Implementação de uma arquitetura de cache de memória (`_socket_buffers`). Agora, ao ler um pacote grande e cortar o `\n`, o sistema salva as mensagens que vieram anexadas nesse pacote em memória para processá-las em seguida, garantindo *Zero Message Loss*.
