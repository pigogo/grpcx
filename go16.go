// +build go1.6,!go1.7

package grpcx

import (
	"net"

	"golang.org/x/net/context"
)

// dialContext connects to the address on the named network.
func dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return (&net.Dialer{Cancel: ctx.Done()}).Dial(network, address)
}
