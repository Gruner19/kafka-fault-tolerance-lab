# Evidência de um acesso

Run: `high-contention-20261109-224151-s4202-r1-6125`  
Processo: `sync-1` · Pedido: `c1-0001`  
Espera local (`ENTER.mono_ns − REQUEST.mono_ns`): **10.935 ms**  
Duração da seção crítica (`END.mono_ns − BEGIN.mono_ns`, observador): **259.464 ms**

## Eventos do nó

| Evento | mono_ns (ms) | offset | queue_head | queue_len |
|---|---:|---:|---|---:|
| REQUEST | 11525385.149 | — | — | — |
| ACQUIRE | 11525395.992 | 0 | c1-0001 | 5 |
| ENTER | 11525396.084 | 0 | c1-0001 | 5 |
| EXIT | 11525655.895 | 0 | c1-0001 | 5 |
| RELEASE | 11525658.503 | 5 | c1-0001 | 5 |
| COMMITTED | 11525658.534 | — | — | — |

## Observador (fora do protocolo)

| Evento | obs_seq | active |
|---|---:|---:|
| BEGIN | 1 | 1 |
| END | 2 | 0 |

## Leitura

Quando `c1-0001` entrou, a fila tinha **5** pedido(s) pendente(s) e a cabeça era `c1-0001`. Como só a cabeça é autorizada, os pedidos seguintes permaneceram na fila até o `RELEASE` correspondente. O `BEGIN` no observador veio depois do `ENTER` e o `RELEASE` só foi publicado após o ACK do `END`.
