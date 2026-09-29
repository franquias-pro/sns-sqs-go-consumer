# SNS → SQS consumer em Go

Exemplo de consumer para eventos SNS entregues em uma fila SQS Standard com **raw message delivery**. O processo lê até 10 mensagens por chamada, usa long polling, impõe um limite de mensagens em voo, trata eventos com workers e confirma em lotes. A meta de **1.000 mensagens/s é uma hipótese de dimensionamento**, que depende da duração do handler, dos limites da conta AWS, da rede, da quantidade de réplicas e do destino do processamento.

## Estrutura

```text
cmd/consumer/          composição e ciclo de vida
internal/config/       configuração e validação
internal/consumer/     polling, workers, confirmação e métricas
internal/handler/      contrato e regra de negócio de exemplo
infra/                 tópico SNS, fila SQS, DLQ e assinatura (Terraform)
examples/              payload de exemplo
```

## Início rápido

Requer Go 1.25+, credenciais AWS via cadeia padrão do SDK e um Elastic APM Server acessível. `infra/` é opcional se tópico, assinatura e fila já existirem.

```bash
cd infra
terraform init
terraform apply
terraform output

cd ..
cp .env.example .env
# Ajuste os valores e carregue as variáveis com seu método preferido.
set -a; source .env; set +a
go mod tidy
go run ./cmd/consumer
```

O SDK usa perfil local, variáveis de ambiente ou role do workload. A role precisa de `sqs:ReceiveMessage` e `sqs:DeleteMessageBatch` na fila. O Terraform de exemplo cria recursos AWS com custo real; confira região e nome antes de aplicar.

Para publicar um evento:

```bash
aws sns publish --region us-east-1 \
  --topic-arn "$(cd infra && terraform output -raw topic_arn)" \
  --message file://examples/order-created.json
```

## Configuração

| Variável | Padrão | Função |
| --- | ---: | --- |
| `SQS_QUEUE_URL` | obrigatório | URL da fila assinante do SNS |
| `AWS_REGION` | `us-east-1` | Região AWS |
| `POLLERS` | `16` | Leituras paralelas, cada uma com até 10 mensagens |
| `WORKERS` | `200` | Máximo de mensagens simultâneas no processo |
| `ACKERS` | `8` | Confirmadores com `DeleteMessageBatch` |
| `PROCESS_TIMEOUT` | `15s` | Prazo individual do handler |
| `VISIBILITY_TIMEOUT` | `120` | Segundos de invisibilidade por recebimento |
| `SHUTDOWN_TIMEOUT` | `45s` | Prazo de drenagem após SIGTERM |
| `LOG_LEVEL` | `info` | `info` ou `debug` |
| `ELASTIC_APM_SERVICE_NAME` | valor do agente | Nome no Elastic APM |
| `ELASTIC_APM_SERVER_URL` | valor do agente | Endpoint do Elastic APM Server |
| `ELASTIC_APM_SECRET_TOKEN` | vazio | Token, se exigido pelo servidor |
| `ELASTIC_APM_TRANSACTION_SAMPLE_RATE` | valor do agente | Exemplo: `0.01` a 1.000/s |
| `ELASTIC_APM_METRICS_INTERVAL` | valor do agente | Exemplo: `30s` |

O arquivo `.env.example` é um modelo; Go não lê `.env` automaticamente. Em produção, injete variáveis via workload e guarde o token como segredo.

## Logs e Elastic APM

- Logs JSON com `slog`: inicialização, resumo a cada 60 segundos (`received_1m`, `processed_1m`, `failed_1m`, `deleted_1m`, `deleted_per_second_1m`, `in_flight`), totais no encerramento e detalhes de cada falha. Sucessos individuais só em `debug` para evitar volume excessivo.
- Cada mensagem tem uma transação `SQS process event`; erros são enviados ao APM. Logs de falha incluem `trace.id` para correlação. Transações terminam após a confirmação SQS, portanto falha de deleção aparece como falha da transação.
- Métricas customizadas no Elastic APM: `consumer.received.total`, `consumer.processed.total`, `consumer.failed.total`, `consumer.deleted.total`, `consumer.delete_failed.total`, `consumer.in_flight`. As métricas `*.total` são contadores cumulativos por instância; calcule a taxa usando a diferença entre amostras. O agente também publica métricas de **Go runtime** (`golang.goroutines`, heap, GC etc.).
- As métricas do runtime Go precisam de visualizações próprias no Kibana. Para saúde da fila e autoscaling, observe também `ApproximateAgeOfOldestMessage`, mensagens visíveis e DLQ no CloudWatch; métricas locais não substituem a profundidade da fila.

## Semântica e capacidade

SQS Standard entrega **ao menos uma vez** e pode duplicar mensagens. Substitua `internal/handler/event.go` por uma operação persistente e **idempotente**, usando `event_id` como chave única no banco ou em um registro de processamento. A demonstração apenas valida e registra o tipo do evento, sem integração de negócio.

Erros de parsing ou de negócio não são confirmados; a mensagem reaparece após o visibility timeout e vai à DLQ após `maxReceiveCount=5`. Uma falha parcial em `DeleteMessageBatch` é verificada por item, e esses itens também serão reenviados. Configure alarmes para idade da fila e mensagens na DLQ. Se um handler ignorar o contexto e exceder seu prazo, o processo pode continuar ocupado; ajuste timeouts das chamadas externas e o visibility timeout de acordo com o pior caso esperado.

Como primeira estimativa, `concorrência ≈ vazão desejada × tempo médio de processamento`: 1.000/s × 0,2s = 200 workers. Com 1 segundo por evento, seriam cerca de 1.000 workers somando todas as réplicas. `POLLERS=16` e `WORKERS=200` são parâmetros iniciais; execute carga sustentada, meça `deleted_per_second_1m`, latência, CPU, memória e idade da fila, e ajuste réplicas e limites. Com 1.000/s sustentados, serão cerca de 86,4 milhões de mensagens/dia; considere custos de SNS, SQS e APM.

As chamadas `ReceiveMessage` têm máximo de 10 mensagens e espera de 20 segundos. O limite de capacidade é reservado antes da leitura, de modo que a visibilidade de uma mensagem recebida não se esgote em um backlog local. As confirmações são agrupadas por até 10 mensagens ou 50 ms. No SIGTERM, o processo interrompe novas leituras, drena mensagens já recebidas e tenta confirmar os sucessos até `SHUTDOWN_TIMEOUT`.

## Referências

- [AWS SDK for Go v2: SQS](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/go_sqs_code_examples.html)
- [AWS: visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [AWS: dead-letter queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [Elastic APM Go: métricas](https://www.elastic.co/docs/reference/apm/agents/go/metrics)
- [Elastic APM Go: instrumentação](https://www.elastic.co/docs/reference/apm/agents/go/custom-instrumentation)
