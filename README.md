# SNS → SQS consumer em Go

Exemplo de consumer para eventos SNS entregues em uma fila SQS Standard com **raw message delivery**. O processo lê até 10 mensagens por chamada, usa long polling, impõe um limite de mensagens em voo, trata eventos com workers e confirma em lotes. A meta de **1.000 mensagens/s é uma hipótese de dimensionamento**, que depende da duração do handler, dos limites da conta AWS, da rede, da quantidade de réplicas e do destino do processamento.

## Estrutura

```text
cmd/consumer/           composição e ciclo de vida
cmd/loadgen/            gerador de carga SNS para medir vazão real
internal/config/        configuração e validação
internal/consumer/      polling, workers e confirmação SQS
internal/handler/       regra de negócio de exemplo
internal/storage/mongodb/ persistência idempotente dos eventos
internal/health/        liveness e readiness HTTP
internal/observability/ logs, contadores e adaptador Elastic APM
infra/                  tópico SNS, fila SQS, DLQ e assinatura (Terraform)
k8s/                    Deployment e configuração Kubernetes
compose.yaml             execução local com MongoDB e LocalStack
examples/               payload de exemplo
```

## Executar localmente com Docker Compose

O Compose sobe MongoDB, LocalStack (SNS, SQS e DLQ) e o consumer. A configuração local desabilita o envio ao Elastic APM; logs JSON e endpoints de saúde continuam ativos.

```bash
docker compose up --build -d
curl -f http://localhost:8080/healthz
curl -f http://localhost:8080/readyz
```

Publique duas vezes o mesmo evento e confira que só um documento foi gravado:

```bash
for i in 1 2; do
  docker compose exec -T localstack awslocal sns publish \
    --topic-arn arn:aws:sns:us-east-1:000000000000:example-events \
    --message '{"event_id":"demo-1","type":"order.created","occurred_at":"2026-09-28T20:00:00Z","data":{"order_id":"ord-1"}}'
done
docker compose exec -T mongo mongosh --quiet events \
  --eval 'db.ingested_events.countDocuments({_id:"demo-1"})'
```

O último comando deve imprimir `1`. Para ver a vazão local: `docker compose --profile load run --rm loadgen`. O perfil publica por 60 segundos com alvo de 1.000 eventos/s; ajuste `RATE_PER_SECOND` no Compose se o computador não acompanhar. Pare com `docker compose down`; `docker compose down -v` também remove os dados locais.

## AWS sem Compose

Requer Go 1.25+, MongoDB acessível, credenciais AWS via cadeia padrão do SDK e um Elastic APM Server acessível. `infra/` é opcional se tópico, assinatura e fila já existirem.

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
| `SQS_ENDPOINT_URL` | vazio | Endpoint alternativo para LocalStack; vazio na AWS |
| `AWS_REGION` | `us-east-1` | Região AWS |
| `MONGO_URI` | obrigatório | URI de conexão MongoDB |
| `MONGO_DATABASE` | `events` | Database dos eventos |
| `MONGO_COLLECTION` | `ingested_events` | Collection dos eventos |
| `MONGO_MAX_POOL_SIZE` | `100` | Tamanho máximo do pool por servidor |
| `HEALTH_ADDR` | `:8080` | Listener HTTP das probes |
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

## Imagem Docker e Kubernetes

`Dockerfile` compila um binário estático e o executa como usuário sem privilégios em uma imagem `scratch`. O build padrão gera o consumer:

```bash
docker build -t SEU_REGISTRY/sns-sqs-go-consumer:1.0.0 .
docker push SEU_REGISTRY/sns-sqs-go-consumer:1.0.0
```

Antes de `kubectl apply -f k8s/consumer.yaml`, troque no manifesto a imagem, a URL da fila e o endpoint APM. Crie um Secret chamado `sns-sqs-go-consumer` com a chave `mongo-uri` (e opcionalmente `apm-secret-token`). Associe o ServiceAccount a uma role IAM com `sqs:ReceiveMessage` e `sqs:DeleteMessageBatch` na fila, por EKS Pod Identity ou IRSA. O manifesto traz duas réplicas como exemplo; ajuste workers, requests e limits com dados do teste de carga. O `terminationGracePeriodSeconds` de 60 s cobre o `SHUTDOWN_TIMEOUT` de 45 s e o flush de telemetria.

- `GET /healthz`: liveness, responde enquanto o processo HTTP está ativo.
- `GET /readyz`: readiness, responde 200 somente durante a operação e com MongoDB acessível; devolve 503 no desligamento ou se o ping falhar.

As probes HTTP são chamadas diretamente no Pod; o consumer não precisa de um Service Kubernetes para recebê-las.

O consumer depende de interfaces pequenas (`Metrics`, `Tracer` e `MessageTrace`). A implementação Elastic APM, os contadores e o resumo periódico de logs ficam em `internal/observability`. Assim, alterações no exportador ou na forma de registrar métricas não exigem mudanças no fluxo de leitura e confirmação da fila.

## Medindo a vazão

O [blueprint de Matheus Fidelis](https://fidelissauro.dev/sqs-consumer-go/) compara leitura e deleção unitárias com lotes, workers e channels. Este projeto usa leitura e confirmação em lotes, além de limitar mensagens em voo e verificar falhas individuais no `DeleteMessageBatch`. Os resultados do artigo medem essencialmente operações SQS; o processamento real pode mudar muito a vazão.

O comando `cmd/loadgen` publica eventos `order.created` no SNS em lotes de 10. Ele exige `sns:Publish` no tópico e gera custo na AWS. Para uma primeira medição de 60 segundos:

```bash
export SNS_TOPIC_ARN="$(cd infra && terraform output -raw topic_arn)"
RATE_PER_SECOND=1000 DURATION=60s PUBLISHERS=20 go run ./cmd/loadgen
```

O gerador informa quantas mensagens foram submetidas, publicadas ou falharam, e sua taxa efetiva. Compare essa taxa com `deleted_per_second_1m` do consumer e com a idade da fila no CloudWatch. Deixe o teste rodar tempo suficiente para medir regime estável; um burst curto pode mascarar acúmulo na fila. `RATE_PER_SECOND` aceita múltiplos de 10 entre 10 e 10.000; se o SNS ou a rede não acompanhar a taxa solicitada, a taxa efetiva será menor.

## Logs e Elastic APM

- Logs JSON com `slog`: inicialização, resumo a cada 60 segundos (`received_1m`, `processed_1m`, `failed_1m`, `deleted_1m`, `deleted_per_second_1m`, `in_flight`), totais no encerramento e detalhes de cada falha. Sucessos individuais só em `debug` para evitar volume excessivo.
- Cada mensagem tem uma transação `SQS process event`; erros são enviados ao APM. Logs de falha incluem `trace.id` para correlação. Transações terminam após a confirmação SQS, portanto falha de deleção aparece como falha da transação.
- Métricas customizadas no Elastic APM: `consumer.received.total`, `consumer.processed.total`, `consumer.failed.total`, `consumer.deleted.total`, `consumer.delete_failed.total`, `consumer.in_flight`. As métricas `*.total` são contadores cumulativos por instância; calcule a taxa usando a diferença entre amostras. O agente também publica métricas de **Go runtime** (`golang.goroutines`, heap, GC etc.).
- As métricas do runtime Go precisam de visualizações próprias no Kibana. Para saúde da fila e autoscaling, observe também `ApproximateAgeOfOldestMessage`, mensagens visíveis e DLQ no CloudWatch; métricas locais não substituem a profundidade da fila.

## Semântica e capacidade

SQS Standard entrega **ao menos uma vez** e pode duplicar mensagens. `internal/storage/mongodb` grava o evento completo em `payload_json`, com `event_id` como `_id` único. Uma tentativa repetida com o mesmo ID retorna sucesso lógico e a mensagem é confirmada no SQS. A regra de exemplo aceita `order.created`; ao adicionar efeitos adicionais (pagamento, e-mail ou escrita em outra collection), implemente-os na mesma fronteira idempotente ou com um padrão transacional apropriado. IDs iguais com conteúdos diferentes são tratados como duplicatas; o primeiro documento permanece.

Erros de parsing ou de negócio não são confirmados; a mensagem reaparece após o visibility timeout e vai à DLQ após `maxReceiveCount=5`. Uma falha parcial em `DeleteMessageBatch` é verificada por item, e esses itens também serão reenviados. Configure alarmes para idade da fila e mensagens na DLQ. Se um handler ignorar o contexto e exceder seu prazo, o processo pode continuar ocupado; ajuste timeouts das chamadas externas e o visibility timeout de acordo com o pior caso esperado.

Como primeira estimativa, `concorrência ≈ vazão desejada × tempo médio de processamento`: 1.000/s × 0,2s = 200 workers. Com 1 segundo por evento, seriam cerca de 1.000 workers somando todas as réplicas. `POLLERS=16` e `WORKERS=200` são parâmetros iniciais; execute carga sustentada, meça `deleted_per_second_1m`, latência, CPU, memória e idade da fila, e ajuste réplicas e limites. Com 1.000/s sustentados, serão cerca de 86,4 milhões de mensagens/dia; considere custos de SNS, SQS e APM.

As chamadas `ReceiveMessage` têm máximo de 10 mensagens e espera de 20 segundos. O limite de capacidade é reservado antes da leitura, de modo que a visibilidade de uma mensagem recebida não se esgote em um backlog local. As confirmações são agrupadas por até 10 mensagens ou 50 ms. No SIGTERM, o processo interrompe novas leituras, drena mensagens já recebidas e tenta confirmar os sucessos até `SHUTDOWN_TIMEOUT`.

## Referências

- [AWS SDK for Go v2: SQS](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/go_sqs_code_examples.html)
- [AWS: visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [AWS: dead-letter queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [Elastic APM Go: métricas](https://www.elastic.co/docs/reference/apm/agents/go/metrics)
- [Elastic APM Go: instrumentação](https://www.elastic.co/docs/reference/apm/agents/go/custom-instrumentation)
- [MongoDB Go Driver: inserções e `_id` único](https://www.mongodb.com/docs/drivers/go/current/crud/insert/)
- [LocalStack: Queue URLs entre containers](https://docs.localstack.cloud/aws/services/sqs/)
- [Kubernetes: probes de saúde](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
