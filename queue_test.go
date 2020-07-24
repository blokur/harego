package harego_test

import (
	"fmt"
	"testing"

	"github.com/blokur/harego"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewExchange(t *testing.T) {
	t.Run("BadInput", testNewExchangeBadInput)
}

func testNewExchangeBadInput(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		msg  string
		conf []harego.ConfigFunc
	}{
		{"connection", []harego.ConfigFunc{
			harego.WithRabbitMQMock(nil),
		}},
		{"workers", []harego.ConfigFunc{
			harego.Workers(0),
		}},
		{"consumer name", []harego.ConfigFunc{
			harego.ConsumerName(""),
		}},
		{"queue name", []harego.ConfigFunc{
			harego.QueueName(""),
		}},
		{"exchange name", []harego.ConfigFunc{
			harego.ExchangeName(""),
		}},
		{"exchange type", []harego.ConfigFunc{
			harego.WithExchangeType(-1),
		}},
		{"exchange type", []harego.ConfigFunc{
			harego.WithExchangeType(9999999),
		}},
		{"prefetch count", []harego.ConfigFunc{
			harego.PrefetchCount(0),
		}},
		{"prefetch size", []harego.ConfigFunc{
			harego.PrefetchSize(-1),
		}},
		{"delivery mode", []harego.ConfigFunc{
			harego.WithDeliveryMode(10),
		}},
	}
	for i, tc := range tcs {
		tc := tc
		name := fmt.Sprintf("%d_%s", i, tc.msg)
		t.Run(name, func(t *testing.T) {
			_, err := harego.NewExchange(nil,
				tc.conf...,
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.msg)
		})
	}
}
