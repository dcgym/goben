// Credit to github/google/cloudprober
// This file is inspired by https://github.com/google/cloudprober/blob/master/probes/ping/ping.go
// I borrowed the idea of using ICMP (ping) message to measure matrices like rtt. The original
// implementation is overkilled for our purpose, so this is a simplified version

package main

import (
	"fmt"
	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

const (
	timeBytesSize = 8
	protocolICMP  = 1
)

type ProberConfig struct {
	proto         string        // the protocol for the ICMP packet connection (ie. ip4:icmp, ip4:1, ip6:58 ...)
	source        string        // the server address
	targets       []string      // the hosts' addresses
	probeInterval time.Duration // probe probeInterval
	pktInterval	  time.Duration // packet sending interval
	pktsPerProbe  int           // number of packets sent per probe
}

type Prober struct {
	config		ProberConfig
	conn		*icmp.PacketConn	// the ICMP connection
	runCnt		uint64				// a counter that helps to construct seq# and runId
}

// Initialize a new Prober instance
// Need to be called before any of the following
func (p *Prober) Init(config ProberConfig) error {
	p.config = config
	return p.listen()
}

// create a new icmp connection to listen to in coming packets
func (p *Prober) listen() error {
	opts := p.config
	if opts.proto == "" {
		log.Fatalf("The prober from host %s misses configuration info.\n", opts.source)
	}
	var err error
	p.conn, err = icmp.ListenPacket(opts.proto, opts.source)
	return err
}

// construct the ICMP message and marshall it to bytes
func (p *Prober) packetToSend(runId, seq uint16) []byte {
	// todo: handle UDP
	msg := &icmp.Message{
		Type: ipv4.ICMPTypeEcho,
		Code: 0,
		Body: &icmp.Echo{
			ID: int(runId),
			Seq: int(seq),
			Data: timeToBytes(time.Now()),
		},
	}
	bytes, err := msg.Marshal(nil)
	if err != nil {
		// This should never happen
		log.Panicf("Error marshalling the ICMP message. Err: %v\n", err)
	}
	return bytes
}

// unmarshall the bytes back to ICMP message
func (p *Prober) packetToRecv(pktbuf []byte) (net.IP, *icmp.Message, error) {
	n, sender, err := p.conn.ReadFrom(pktbuf)
	if err != nil {
		return nil, nil, err
	}
	// get the sender's IP address
	// Since sender is an interface net.Addr, we have to cast it down to net.IPAddr/net.UDPAddr type to get the IP
	var senderIP net.IP
	switch sender := sender.(type) {
	case *net.IPAddr:
		senderIP = sender.IP
	case *net.UDPAddr:
		senderIP = sender.IP
	}

	msg, err := icmp.ParseMessage(protocolICMP, pktbuf[:n])
	if err != nil {
		return nil, nil, err
	}
	return senderIP, msg, nil
}

// send ICMP message
// We sent the icmp packets synchronously
func (p *Prober) send(runID uint16, morePkts chan bool) {
	seq := runID & uint16(0xff00)
	for i := 0; i < p.config.pktsPerProbe; i++ {
		for _, target := range p.config.targets {
			log.Printf("Request to=%s id=%d seq=%d", target, runID, seq)
			if _, err := p.conn.WriteTo(p.packetToSend(runID, seq), parseIP(target)); err != nil {
				log.Println(err.Error())
				continue
			}
			fmt.Println("SEND A ICMP PACKET")
			morePkts <- true
		}
		seq++
		time.Sleep(p.config.pktInterval)
	}
	log.Printf("%s: Done sending packets, closing the tracker.", p.config.source)
	close(morePkts)
}

// receive ICMP message and compute the measurements
func (p *Prober) recv(runID uint16, morePkts chan bool) {
	// keep track if the packet arrived has been received before
	received := make(map[string]bool)
	// a counter to make sure we read all packets arrived (including the outstanding
	// ones after the sender has closed the connection)
	outstandingPkts := 0
	// the byte stream buffer
	pktbuf := make([]byte, 1500)

	for {
		if outstandingPkts == 0 {
			if _, ok := <-morePkts; ok {
				outstandingPkts++
			} else {
				return
			}
		}
		senderIP, msg, err := p.packetToRecv(pktbuf)
		if err != nil {
			log.Printf("Unmarshalling icmp message Error: %s\n", err.Error())
			if neterr, ok := err.(*net.OpError); ok && neterr.Timeout() {
				return
			}
		}
		target := senderIP.String()
		echoMsg, ok := msg.Body.(*icmp.Echo)
		if !ok {
			log.Println("Got wrong packet in ICMP echo reply.") // should never happen
			continue
		}

		// get rtt
		rtt := time.Since(bytesToTime(echoMsg.Data))

		// check if this packet belong to this run
		if !matchPacket(runID, echoMsg.ID, echoMsg.Seq) {
			log.Printf(
				"Reply from=%s id=%d seq=%d rtt=%s Unmatched packet, probably from the last probe run.\n",
				target, echoMsg.ID, echoMsg.Seq, rtt)
			continue
		}

		// check if we have seen this packet before
		pktID := fmt.Sprintf("%s_%d", target, echoMsg.Seq)
		if received[pktID] {
			log.Printf("Duplicate reply from=%s id=%d seq=%d rtt=%s\n", target, echoMsg.ID, echoMsg.Seq, rtt)
			continue
		}

		// record the rrt
		// todo: dump this measure to a file
		log.Printf("RTT: src=%s, dst=%s, rtt=%s\n", p.config.source, target, rtt)

		// bookkeeping
		received[pktID] = true
		outstandingPkts--
	}
}

// runProbe is called by Start for each probe probeInterval and perform a single run
func (p *Prober) runProbe() {
	p.runCnt++
	runID := p.generateRunId()
	wg := new(sync.WaitGroup)
	wg.Add(1)
	// morePtks is a channel used to let the receiver know when there are no more packets
	morePkts := make(chan bool, int(p.config.pktsPerProbe * len(p.config.targets)))
	go func() {
		defer wg.Done()
		p.recv(runID, morePkts)
	}()
	p.send(runID, morePkts)
	wg.Wait()
	log.Printf("The prober from host %s finished!\n", p.config.source)
}

// Start starts the prober and perform a probe for each probeInterval
// Start must be called after Init() is called
func (p *Prober) Start() {
	if p.conn == nil {
		log.Panicf("The prober from host %s is not properly initialized.\n", p.config.source)
	}
	defer p.conn.Close()
	for range time.Tick(p.config.probeInterval) {
		p.runProbe()
	}
}

/* Helpers */

// generate unique run id
// runId and seq is formed using a 16-bit structure:
/* runCnt (== base of seq) |	random number
  -- -- -- -- -- -- -- --  | -- -- -- -- -- -- -- --
-	the first byte is the runCnt so that we can distinguish the packets from different runs
	and this will be used as the base number for seq numbers for each run
-	the second byte is a random number to make sure the runIds are unique among many Prober
	instances
*/
func (p *Prober) generateRunId() uint16 {
	return (uint16(p.runCnt) << 8) + uint16(rand.Intn(0x00ff))
}

// check if the packet belong to a certain run
func matchPacket(runId uint16, pktId, seq int) bool {
	return (runId == uint16(pktId)) && (runId>>8 == uint16(seq)>>8)
}

// parse host ip string to net.Addr
func parseIP(host string) net.Addr {
	// todo: handle udp
	ip := net.ParseIP(host)
	var addr net.Addr
	addr = &net.IPAddr{IP: ip}
	return addr
}

// serialize time to byte stream
func timeToBytes(t time.Time) []byte {
	nsec := t.UnixNano()
	var timeBytes [timeBytesSize]byte
	for i := uint8(0); i < timeBytesSize; i++ {
		// To get timeBytes:
		// 0th byte - shift bits by 56 (7*8) bits, AND with 0xff to get the last 8 bits
		// 1st byte - shift bits by 48 (6*8) bits, AND with 0xff to get the last 8 bits
		// ... ...
		// 7th byte - shift bits by 0 (0*8) bits, AND with 0xff to get the last 8 bits
		timeBytes[i] = byte((nsec >> ((timeBytesSize - i - 1) * timeBytesSize)) & 0xff)
	}
	return timeBytes[:]
}

// deserialize byte stream to time
func bytesToTime(b []byte) time.Time {
	var nsec int64
	for i := uint8(0); i < timeBytesSize; i++ {
		nsec += int64(b[i]) << ((timeBytesSize - i - 1) * timeBytesSize)
	}
	return time.Unix(0, nsec)
}