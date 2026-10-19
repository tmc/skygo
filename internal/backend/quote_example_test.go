package backend_test

import (
	"fmt"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

func ExampleValidateAdvice() {
	amount := int64(0)
	observed := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	advice := &backend.TargetAdvice{Quote: &backend.Quote{Rate: backend.Money{AmountMicros: &amount, Currency: "USD"}, Unit: backend.TargetHour, Origin: backend.Configured, Source: "operator:example", ObservedAt: observed, ValidUntil: observed.Add(time.Hour)}}
	fmt.Println(backend.ValidateAdvice(advice))
	copy := backend.CloneAdvice(advice)
	*copy.Quote.Rate.AmountMicros = 10
	fmt.Println(*advice.Quote.Rate.AmountMicros)
	// Output:
	// <nil>
	// 0
}

func ExampleCloneAdvice() {
	fmt.Println(backend.CloneAdvice(nil) == nil)
	// Output: true
}

func ExampleValidQuoteCurrency() {
	fmt.Println(backend.ValidQuoteCurrency("USD"), backend.ValidQuoteCurrency("usd"))
	// Output: true false
}

func ExampleValidRateUnit() {
	fmt.Println(backend.ValidRateUnit(backend.TargetHour), backend.ValidRateUnit(backend.RateUnitUnspecified))
	// Output: true false
}
