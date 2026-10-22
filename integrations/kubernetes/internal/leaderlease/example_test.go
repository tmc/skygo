package leaderlease_test

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/tmc/skygo/integrations/kubernetes/internal/leaderlease"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// The fake clientset is a labeled model, not a cluster.
func Example() {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	h, err := leaderlease.Acquire(ctx, client, leaderlease.Config{
		Namespace: "skygo", Name: "skygo-server", Identity: "skygo-0_0123456789abcdef",
		LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 2 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	lease, err := client.CoordinationV1().Leases("skygo").Get(ctx, "skygo-server", metav1.GetOptions{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(*lease.Spec.HolderIdentity, h.Valid(time.Now()))

	// Open and use the state; on <-h.Lost() stop using it. Release only
	// after it was closed successfully; otherwise h.Stop() and let the
	// Lease expire.
	if err := h.Release(ctx); err != nil {
		log.Fatal(err)
	}
	lease, err = client.CoordinationV1().Leases("skygo").Get(ctx, "skygo-server", metav1.GetOptions{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%q %v\n", *lease.Spec.HolderIdentity, h.Err())
	// Output:
	// skygo-0_0123456789abcdef true
	// "" leaderlease: lease released
}

func ExampleNewIdentity() {
	id, err := leaderlease.NewIdentity("skygo-0")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(strings.HasPrefix(id, "skygo-0_"), len(id))
	// Output: true 24
}
