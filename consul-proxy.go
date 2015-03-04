package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"time"

	"code.google.com/p/go-uuid/uuid"

	"github.com/hashicorp/consul/api"
)

var connid = uint64(0)
var listenAddress = flag.String("listen", "127.0.0.1:8000", "Listen address")
var consulServer = flag.String("consul", "127.0.0.1:8500", "Address of Consul Server")
var service = flag.String("service", "upstream", "Service name in Consul")
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
		p.log <- fmt.Sprintf("%s No backend servers available!", p.prefix)
		return
	}
	order := rand.Perm(len(nodes))
	for i := range order {
		node := nodes[i]
		remoteAddr := fmt.Sprintf("%s:%d", node.Address, node.ServicePort)
		raddr, err := net.ResolveTCPAddr("tcp", remoteAddr)
		if err == nil {
			rconn, err := net.DialTCP("tcp", nil, raddr)
			if err == nil {
				p.raddr = raddr
				p.rconn = rconn
				break
			} else {
				p.log <- fmt.Sprintf("%s Error connecting to %s: %s", p.prefix, remoteAddr, err.Error())
			}
		} else {
			p.log <- fmt.Sprintf("%s Error resolving %s: %s", p.prefix, remoteAddr, err.Error())
		}
		if i+1 == len(nodes) {
			p.log <- fmt.Sprintf("%s Could not connect to any upstream servers!", p.prefix)
			return
		}
	}

	defer p.rconn.Close()
	//display both ends
	p.log <- fmt.Sprintf("%s Opened %s >>> %s", p.prefix, p.lconn.RemoteAddr().String(), p.rconn.RemoteAddr().String())
	//bidirectional copy
	go p.pipe(p.lconn, p.rconn)
	go p.pipe(p.rconn, p.lconn)
	//wait for close...
	<-p.errsig
	p.log <- fmt.Sprintf("%s Closed (%d bytes sent, %d bytes recieved)", p.prefix, p.sentBytes, p.receivedBytes)
}

func (p *proxy) pipe(src, dst *net.TCPConn) {
	//data direction
	var f, h string
	islocal := src == p.lconn
	if *verbose {
		if islocal {
			f = ">>> %d bytes sent%s"
		} else {
			f = "<<< %d bytes recieved%s"
		}
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
		} else if *verbose {
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

	laddr, err := net.ResolveTCPAddr("tcp", *listenAddress)
	check(err, mc)

	listener, err := net.ListenTCP("tcp", laddr)
	check(err, mc)

	consulChannel := make(chan []*api.CatalogService, 1)

	options := &api.QueryOptions{}
	config := api.DefaultConfig()
	config.Address = *consulServer
	client, _ := api.NewClient(config)
	go consulQuery(*service, "", client, options, consulChannel)

	go logger(mc)

	var nodes []*api.CatalogService
	mc <- "Getting upstream nodes from Consul..."
	for {
		nodes = <-consulChannel
		if len(nodes) > 0 {
			for _, node := range nodes {
				fmt.Println(node.Address)
			}
			break
		}
	}

	mc <- fmt.Sprintf("Starting listener on '%s'\n", *listenAddress)
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
			prefix: fmt.Sprintf("Connection %s: ", uuid.NewRandom()),
			log:    mc,
		}
		go p.start(nodes)
	}
}
