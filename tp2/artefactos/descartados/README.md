# Runs descartados

Artefatos mantidos apenas para rastreabilidade do desenvolvimento. Não entram
no agregado oficial nem no relatório entregue.

- `extension-semaphore-20261104-190453-s7505-r1-faab`: primeira execução da
  extensão semáforo (K=2), anterior à correção de `QueueState.apply_release`
  para aceitar qualquer posição dentro da janela autorizada. Falhou em V6 por
  uma regra ainda exclusiva de mutex (cabeça da fila).
