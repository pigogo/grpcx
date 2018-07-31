// +build go1.7

package grpcx

import (
	"context"
	"net"
)

// dialContext connects to the address on the named network.
func dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, address)
}
