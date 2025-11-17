package coap

import (
	"context"
	"log"
	"net"

	"github.com/OpenFogStack/tinyFaaS/pkg/rproxy"
	"github.com/pfandzelter/go-coap"
)

const async = false

func Start(ctx context.Context, r *rproxy.RProxy, listenAddr string) {

	h := coap.FuncHandler(
		func(l *net.UDPConn, a *net.UDPAddr, m *coap.Message) *coap.Message {

			log.Printf("have request: %+v", m)
			log.Printf("is confirmable: %v", m.IsConfirmable())
			log.Printf("path: %s", m.PathString())

			p := m.PathString()

			for p != "" && p[0] == '/' {
				p = p[1:]
			}

			log.Printf("have request for path: %s (async: %v)", p, async)

			s, res := r.Call(p, m.Payload, async, nil)

			mes := &coap.Message{
				Type:      coap.Acknowledgement,
				MessageID: m.MessageID,
				Token:     m.Token,
			}

			switch s {
			case rproxy.StatusOK:
				mes.SetOption(coap.ContentFormat, coap.TextPlain)
				mes.Code = coap.Content
				mes.Payload = res
			case rproxy.StatusAccepted:
				mes.Code = coap.Created
			case rproxy.StatusNotFound:
				mes.Code = coap.NotFound
			case rproxy.StatusError:
				mes.Code = coap.InternalServerError
			}

			return mes
		})

	go func() {
		log.Println("CoAP server started")
		if err := coap.ListenAndServe("udp", listenAddr, h); err != nil {
			log.Printf("CoAP server error: %v", err)
		}
		log.Print("CoAP server stopped")
	}()

	// Note: CoAP (UDP) doesn't have built-in graceful shutdown
	// The server will stop when the process exits
	<-ctx.Done()
	log.Println("CoAP server shutdown requested")
}
