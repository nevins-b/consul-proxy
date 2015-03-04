package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"time"

	"github.com/hashicorp/consul/api"
)

var connid = uint64(0)
var port = flag.String("p", ":8000", "Listen Port")
var key = flag.String("k", "upstream", "Consul key of backend")
var verbose = flag.Bool("v", false, "display server actions")
var veryverbose = flag.Bool("vv", false, "display server actions and all tcp data")

type proxy struct {
	sentBytes     uint64
	receivedBytes uint64
	laddr, raddr  *net.TCPAddr
	lconn, rconn  *net.TCPConn
	erred         bool
	errsig        chan bool
	prefix        string
	log           chan string
}

func (p *proxy) err(s string, err error) {
	if p.erred {
		return
	}
	if err != io.EOF {
		p.log <- p.prefix + s
	}
	p.errsig <- true
	p.erred = true
}

func (p *proxy) start(nodes []*api.CatalogService) {
	defer p.lconn.Close()
	if len(nodes) == 0 {
		p.log <- fmt.Sprintf("No backend servers available!")
		return
	}
	order := rand.Perm(len(nodes))
	for _, i := range order {
		node := nodes[i]
		remoteAddr := fmt.Sprintf("%s:%d", node.Address, node.ServicePort)
		raddr, err := net.ResolveTCPAddr("tcp", remoteAddr)
		if err == nil {
			rconn, err := net.DialTCP("tcp", nil, p.raddr)
			if err == nil {
				p.raddr = raddr
				p.rconn = rconn
				break
			}
		}
		if i == len(nodes) {
			p.log <- "Could not connect to any upstream servers!"
			return
		}
	}

	defer p.rconn.Close()
	//display both ends
	p.log <- fmt.Sprintf("Opened %s >>> %s", p.lconn.RemoteAddr().String(), p.rconn.RemoteAddr().String())
	//bidirectional copy
	go p.pipe(p.lconn, p.rconn)
	go p.pipe(p.rconn, p.lconn)
	//wait for close...
	<-p.errsig
	p.log <- fmt.Sprintf("Closed (%d bytes sent, %d bytes recieved)", p.sentBytes, p.receivedBytes)
}

func (p *proxy) pipe(src, dst *net.TCPConn) {
	//data direction
	var f, h string
	islocal := src == p.lconn
	if islocal {
		f = ">>> %d bytes sent%s"
	} else {
		f = "<<< %d bytes recieved%s"
	}
	h = "%s"

	//directional copy
	buff := make([]byte, 0xffff)
	src.SetReadBuffer(len(buff))
	for {
		n, err := src.Read(buff)
		if err != nil {
			p.err("Read failed '%s'\n", err)
			return
		}

		b := buff[:n]

		//show output
		if *veryverbose {
			p.log <- fmt.Sprintf(f, n, "\n"+fmt.Sprintf(h, b))
		} else {
			p.log <- fmt.Sprintf(f, n, "")
		}
		//write out result
		n, err = dst.Write(b)
		if err != nil {
			p.err("Write failed '%s'\n", err)
			return
		}
		if islocal {
			p.sentBytes += uint64(n)
		} else {
			p.receivedBytes += uint64(n)
		}
	}
}

//helper functions

func check(err error, mc chan string) {
	if err != nil {
		mc <- err.Error()
		//os.Exit(1)
	}
}

func logger(mc chan string) {
	for {
		msg := <-mc
		fmt.Printf(msg + "\n")
	}

}

func consulQuery(service string, tag string, client *api.Client, options *api.QueryOptions, channel chan []*api.CatalogService) {
	catalog := client.Catalog()

	for {
		nodes, _, err := catalog.Service(service, tag, options)
		if err != nil {
			panic(err)
		}
		channel <- nodes
		time.Sleep(5 * time.Millisecond)
	}
}

func main() {
	flag.Parse()

	mc := make(chan string)

	laddr, err := net.ResolveTCPAddr("tcp", *port)
	check(err, mc)

	listener, err := net.ListenTCP("tcp", laddr)
	check(err, mc)

	consulChannel := make(chan []*api.CatalogService, 1)

	options := &api.QueryOptions{}
	client, _ := api.NewClient(api.DefaultConfig())
	go consulQuery(*key, "", client, options, consulChannel)
	go logger(mc)

	var nodes []*api.CatalogService
	mc <- "Getting upstream nodes from Consul..."
	for {
		nodes = <-consulChannel
		if len(nodes) > 0 {
			break
		}
	}

	mc <- fmt.Sprintf("Starting listener on '%s'\n", *port)
	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			mc <- fmt.Sprintf("Failed to accept connection '%s'\n", err)
			continue
		}
		connid++

		select {
		case n := <-consulChannel:
			nodes = n
		default:
		}

		p := &proxy{
			lconn:  conn,
			laddr:  laddr,
			erred:  false,
			errsig: make(chan bool),
			prefix: fmt.Sprintf("Connection #%03d ", connid),
			log:    mc,
		}
		go p.start(nodes)
	}
}
