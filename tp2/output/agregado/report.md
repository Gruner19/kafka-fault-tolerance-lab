# TP2 — B6 sobre Kafka: relatório agregado

Fonte: `output`. Cada linha resume um cenário; tempos de espera usam `ENTER.mono_ns − REQUEST.mono_ns` medidos **no mesmo processo**.

## Visão geral

| Cenário | Runs | PASS | K | Nós | Clientes | Pedidos (mediana) | Espera mediana (s) | Espera mín (s) | Espera máx (s) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| control-no-sync | 3 | 3 | 1 | 5 | 5 | 54.0 | 0.000 | 0.000 | 0.004 |
| extension-semaphore | 2 | 2 | 2 | 5 | 5 | 59.0 | 0.889 | 0.003 | 1.737 |
| extension-semaphore-k3 | 2 | 2 | 3 | 5 | 5 | 65.5 | 0.327 | 0.001 | 0.852 |
| high-contention | 3 | 3 | 1 | 5 | 5 | 62.0 | 2.248 | 0.002 | 3.528 |
| normal | 3 | 3 | 1 | 5 | 5 | 192.0 | 0.138 | 0.001 | 2.813 |
| scale-10 | 2 | 2 | 1 | 10 | 10 | 123.5 | 3.132 | 0.002 | 5.676 |
| uneven-load | 3 | 3 | 1 | 5 | 5 | 86.0 | 0.730 | 0.001 | 2.791 |

`K=1` corresponde ao mutex obrigatório sobre `R`; `K>1` identifica a extensão semáforo, executada em etapa separada.

## Mensagens por entrada

Convenção: 1 publicação = 1 mensagem; cada entrega a cada consumidor = 1 mensagem (inclui o próprio publicador). Teórico do B6: **2N + 2** (2 publicações + 2N entregas).

| Cenário | N | Observado (média) | Teórico 2N+2 |
|---|---:|---:|---:|
| extension-semaphore | 5 | 12.000 | 12 |
| extension-semaphore-k3 | 5 | 12.000 | 12 |
| high-contention | 5 | 12.000 | 12 |
| normal | 5 | 12.000 | 12 |
| scale-10 | 10 | 22.000 | 22 |
| uneven-load | 5 | 12.000 | 12 |

## Acessos por cliente

| Cenário | Cliente | Acessos |
|---|---|---:|
| control-no-sync | c1 | 33 |
| control-no-sync | c2 | 33 |
| control-no-sync | c3 | 32 |
| control-no-sync | c4 | 34 |
| control-no-sync | c5 | 31 |
| extension-semaphore | c1 | 26 |
| extension-semaphore | c2 | 23 |
| extension-semaphore | c3 | 23 |
| extension-semaphore | c4 | 21 |
| extension-semaphore | c5 | 25 |
| extension-semaphore-k3 | c1 | 26 |
| extension-semaphore-k3 | c2 | 30 |
| extension-semaphore-k3 | c3 | 26 |
| extension-semaphore-k3 | c4 | 26 |
| extension-semaphore-k3 | c5 | 23 |
| high-contention | c1 | 38 |
| high-contention | c2 | 36 |
| high-contention | c3 | 39 |
| high-contention | c4 | 40 |
| high-contention | c5 | 32 |
| normal | c1 | 141 |
| normal | c2 | 97 |
| normal | c3 | 115 |
| normal | c4 | 116 |
| normal | c5 | 76 |
| scale-10 | c1 | 24 |
| scale-10 | c10 | 25 |
| scale-10 | c2 | 24 |
| scale-10 | c3 | 25 |
| scale-10 | c4 | 24 |
| scale-10 | c5 | 26 |
| scale-10 | c6 | 24 |
| scale-10 | c7 | 24 |
| scale-10 | c8 | 26 |
| scale-10 | c9 | 25 |
| uneven-load | c1 | 120 |
| uneven-load | c2 | 34 |
| uneven-load | c3 | 32 |
| uneven-load | c4 | 37 |
| uneven-load | c5 | 32 |

## Runs e sementes

| Cenário | Run | Semente | PASS |
|---|---|---:|:---:|
| control-no-sync | `control-no-sync-20261104-184248-s3101-r1-971f` | 3101 | sim |
| control-no-sync | `control-no-sync-20261104-184301-s3102-r2-e0bf` | 3102 | sim |
| control-no-sync | `control-no-sync-20261104-184314-s3103-r3-1c4e` | 3103 | sim |
| extension-semaphore | `extension-semaphore-20261109-152912-s7505-r1-805f` | 7505 | sim |
| extension-semaphore | `extension-semaphore-20261109-152939-s7506-r2-6b90` | 7506 | sim |
| extension-semaphore-k3 | `extension-semaphore-k3-20261109-153113-s8505-r1-08d2` | 8505 | sim |
| extension-semaphore-k3 | `extension-semaphore-k3-20261109-153133-s8506-r2-06ff` | 8506 | sim |
| high-contention | `high-contention-20261104-185234-s4202-r1-c0fe` | 4202 | sim |
| high-contention | `high-contention-20261104-185313-s4203-r2-962d` | 4203 | sim |
| high-contention | `high-contention-20261104-185400-s4204-r3-c96d` | 4204 | sim |
| normal | `normal-20261104-184328-s2026-r1-3e3a` | 2026 | sim |
| normal | `normal-20261104-184646-s2027-r2-2479` | 2027 | sim |
| normal | `normal-20261104-184954-s2028-r3-eef8` | 2028 | sim |
| scale-10 | `scale-10-20261104-185729-s6404-r1-8b36` | 6404 | sim |
| scale-10 | `scale-10-20261104-185858-s6405-r2-01c9` | 6405 | sim |
| uneven-load | `uneven-load-20261104-185442-s5303-r1-7721` | 5303 | sim |
| uneven-load | `uneven-load-20261104-185539-s5304-r2-ec93` | 5304 | sim |
| uneven-load | `uneven-load-20261104-185635-s5305-r3-ec7d` | 5305 | sim |

## Modo control

No modo control (`sync_enabled=false`) a corrida é considerada **bem-sucedida quando V1 falha de propósito**: o observador deve registrar `overlaps > 0`, provando que a instrumentação detecta acessos simultâneos.

- `control-no-sync`: 3/3 runs com sobreposição detectada.

## Limitações

- Os experimentos são evidência, não prova: a exclusão mútua decorre do argumento da fila replicada (seção 3.3 da spec).
- `aggregate.csv` contém os mesmos números em formato de máquina; `meta.json` de cada run registra versões, sementes e configuração do tópico.
- Não comparar runs com duração de seção crítica ou carga diferentes.
