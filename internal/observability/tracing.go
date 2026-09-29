package observability

import (
	"go.elastic.co/apm/v2"

	"github.com/mamartins1997/sns-sqs-go-consumer/internal/consumer"
)

type ElasticTracer struct { tracer *apm.Tracer }

func NewElasticTracer(tracer *apm.Tracer) *ElasticTracer {
	return &ElasticTracer{tracer: tracer}
}

func (e *ElasticTracer) StartMessage() consumer.MessageTrace {
	tx := e.tracer.StartTransaction("SQS process event", "messaging")
	return &MessageTransaction{tracer: e.tracer, tx: tx}
}

type MessageTransaction struct {
	tracer *apm.Tracer
	tx     *apm.Transaction
}

func (m *MessageTransaction) TraceID() string {
	return m.tx.TraceContext().Trace.String()
}

func (m *MessageTransaction) End(err error) {
	if err != nil {
		m.tx.Result = "failure"
		apmError := m.tracer.NewError(err)
		apmError.SetTransaction(m.tx)
		apmError.Send()
	} else {
		m.tx.Result = "success"
	}
	m.tx.End()
}
