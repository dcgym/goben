package main

import (
	"bytes"
	"crypto/tls"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"time"
	"unicode"
)

func open(app *config) {

	var proto string
	if app.udp {
		proto = "udp"
	} else {
		proto = "tcp"
	}

	var wg sync.WaitGroup

	for _, h := range app.hosts {

		hh := appendPortIfMissing(h, app.defaultPort)

		for i := 0; i < app.connections; i++ {

			log.Printf("open: opening TLS=%v %s %d/%d: %s", app.tls, proto, i, app.connections, hh)

			if !app.udp && app.tls {
				// try TLS first
				log.Printf("open: trying TLS")
				conn, errDialTLS := tlsDial(proto, hh)
				if errDialTLS == nil {
					spawnClient(app, &wg, conn, i, app.connections, true, h)
					continue
				}
				log.Printf("open: trying TLS: failure: %s: %s: %v", proto, hh, errDialTLS)
			}

			if !app.udp {
				log.Printf("open: trying non-TLS TCP")
			}

			conn, errDial := net.Dial(proto, hh)
			if errDial != nil {
				log.Printf("open: dial %s: %s: %v", proto, hh, errDial)
				continue
			}
			spawnClient(app, &wg, conn, i, app.connections, false, h)
		}
	}

	wg.Wait()
}

func spawnClient(app *config, wg *sync.WaitGroup, conn net.Conn, c, connections int, isTLS bool, targetHost string) {
	wg.Add(1)
	go handleConnectionClient(app, wg, conn, c, connections, isTLS)
	go handleMeasurement(app, targetHost)
}

func tlsDial(proto, h string) (net.Conn, error) {
	conf := &tls.Config{
		InsecureSkipVerify: true,
	}

	conn, err := tls.Dial(proto, h, conf)

	return conn, err
}

// ExportInfo records data for export
type ExportInfo struct {
	Input  ChartData
	Output ChartData
}

func sendOptions(app *config, conn io.Writer) error {
	opt := app.opt
	if app.udp {
		var optBuf bytes.Buffer
		enc := gob.NewEncoder(&optBuf)
		if errOpt := enc.Encode(&opt); errOpt != nil {
			log.Printf("handleConnectionClient: UDP options failure: %v", errOpt)
			return errOpt
		}
		_, optWriteErr := conn.Write(optBuf.Bytes())
		if optWriteErr != nil {
			log.Printf("handleConnectionClient: UDP options write: %v", optWriteErr)
			return optWriteErr
		}
	} else {
		enc := gob.NewEncoder(conn)
		if errOpt := enc.Encode(&opt); errOpt != nil {
			log.Printf("handleConnectionClient: TCP options failure: %v", errOpt)
			return errOpt
		}
	}
	return nil
}

func handleConnectionClient(app *config, wg *sync.WaitGroup, conn net.Conn, c, connections int, isTLS bool) {
	defer wg.Done()

	log.Printf("handleConnectionClient: starting %s %d/%d %v", protoLabel(isTLS), c, connections, conn.RemoteAddr())

	// send options
	if errOpt := sendOptions(app, conn); errOpt != nil {
		return
	}
	opt := app.opt
	log.Printf("handleConnectionClient: options sent: %v", opt)

	// receive ack
	//log.Printf("handleConnectionClient: FIXME WRITEME server does not send ack for UDP")
	if !app.udp {
		var a ack
		if errAck := ackRecv(app.udp, conn, &a); errAck != nil {
			log.Printf("handleConnectionClient: receiving ack: %v", errAck)
			return
		}
		log.Printf("handleConnectionClient: %s ack received", protoLabel(isTLS))
	}

	doneReader := make(chan struct{})
	doneWriter := make(chan struct{})

	info := ExportInfo{
		Input:  ChartData{},
		Output: ChartData{},
	}

	var input *ChartData
	var output *ChartData

	if app.csv != "" || app.export != "" || app.chart != "" || app.ascii {
		input = &info.Input
		output = &info.Output
	}

	go clientReader(conn, c, connections, doneReader, opt, input)
	if !app.passiveClient {
		go clientWriter(conn, c, connections, doneWriter, opt, output)
	}

	tickerPeriod := time.NewTimer(app.opt.TotalDuration)

	<-tickerPeriod.C
	log.Printf("handleConnectionClient: %v timer", app.opt.TotalDuration)

	tickerPeriod.Stop()

	conn.Close() // force reader/writer to quit

	<-doneReader // wait reader exit
	if !app.passiveClient {
		<-doneWriter // wait writer exit
	}

	if app.csv != "" {
		filename := fmt.Sprintf(app.csv, c, conn.RemoteAddr())
		log.Printf("exporting CSV test results to: %s", filename)
		errExport := exportCsv(filename, &info)
		if errExport != nil {
			log.Printf("handleConnectionClient: export CSV: %s: %v", filename, errExport)
		}
	}

	if app.export != "" {
		filename := fmt.Sprintf(app.export, c, conn.RemoteAddr())
		log.Printf("exporting YAML test results to: %s", filename)
		errExport := export(filename, &info)
		if errExport != nil {
			log.Printf("handleConnectionClient: export YAML: %s: %v", filename, errExport)
		}
	}

	if app.chart != "" {
		filename := fmt.Sprintf(app.chart, c, conn.RemoteAddr())
		log.Printf("rendering chart to: %s", filename)
		errRender := chartRender(filename, &info.Input, &info.Output)
		if errRender != nil {
			log.Printf("handleConnectionClient: render PNG: %s: %v", filename, errRender)
		}
	}

	plotascii(&info, conn.RemoteAddr().String(), c)

	log.Printf("handleConnectionClient: closing: %d/%d %v", c, connections, conn.RemoteAddr())
}

// init and start Prober
func handleMeasurement(app *config, targetHost string) {
		proto := "ip4:icmp" // currently we only handel ipv4 tcp
		probeInterval, pktInterval, pktPerProbe := validateProberConfig(app.probeInterval, app.pktInterval, app.pktPerProbe)
		source, er := GetSourceIP()
		if er != nil {
			log.Panicf("Cannot resolve the host machine IP. %v", er.Error())
		}
		proberConfig := ProberConfig {
			proto,
			source,		 	// default is the local machine's external IP
			[]string{targetHost},		// hacky way to fit prober.go's API | todo clean up the prober API
			probeInterval,
			pktInterval,
			pktPerProbe,
		}
		var prober Prober
		err := prober.Init(proberConfig)
		if err != nil {
			log.Panicf("Cannot initialize the prober! %v \n", err.Error())
		}
		prober.Start()
}

func clientReader(conn net.Conn, c, connections int, done chan struct{}, opt options, stat *ChartData) {
	log.Printf("clientReader: starting: %d/%d %v", c, connections, conn.RemoteAddr())

	connIndex := fmt.Sprintf("%d/%d", c, connections)

	buf := make([]byte, opt.ReadSize)

	workLoop(connIndex, "clientReader", "rcv/s", conn.Read, buf, opt.ReportInterval, 0, stat)

	close(done)

	log.Printf("clientReader: exiting: %d/%d %v", c, connections, conn.RemoteAddr())
}

func clientWriter(conn net.Conn, c, connections int, done chan struct{}, opt options, stat *ChartData) {
	log.Printf("clientWriter: starting: %d/%d %v", c, connections, conn.RemoteAddr())

	connIndex := fmt.Sprintf("%d/%d", c, connections)

	buf := randBuf(opt.WriteSize)

	workLoop(connIndex, "clientWriter", "snd/s", conn.Write, buf, opt.ReportInterval, opt.MaxSpeed, stat)

	close(done)

	log.Printf("clientWriter: exiting: %d/%d %v", c, connections, conn.RemoteAddr())
}

func randBuf(size int) []byte {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		log.Printf("randBuf error: %v", err)
	}
	return buf
}

type call func(p []byte) (n int, err error)

type account struct {
	prevTime  time.Time
	prevSize  int64
	prevCalls int
	size      int64
	calls     int
}

// ChartData records data for chart
type ChartData struct {
	XValues []time.Time
	YValues []float64
}

const fmtReport = "%s %7s %14s rate: %6d Mbps %6d %s"

func (a *account) update(n int, reportInterval time.Duration, conn, label, cpsLabel string, stat *ChartData) {
	a.calls++
	a.size += int64(n)

	now := time.Now()
	elap := now.Sub(a.prevTime)
	if elap > reportInterval {
		elapSec := elap.Seconds()
		mbps := float64(8*(a.size-a.prevSize)) / (1000000 * elapSec)
		cps := int64(float64(a.calls-a.prevCalls) / elapSec)
		log.Printf(fmtReport, conn, "report", label, int64(mbps), cps, cpsLabel)
		a.prevTime = now
		a.prevSize = a.size
		a.prevCalls = a.calls

		// save chart data
		if stat != nil {
			stat.XValues = append(stat.XValues, now)
			stat.YValues = append(stat.YValues, mbps)
		}
	}
}

func (a *account) average(start time.Time, conn, label, cpsLabel string) {
	elapSec := time.Since(start).Seconds()
	mbps := int64(float64(8*a.size) / (1000000 * elapSec))
	cps := int64(float64(a.calls) / elapSec)
	log.Printf(fmtReport, conn, "average", label, mbps, cps, cpsLabel)
}

func workLoop(conn, label, cpsLabel string, f call, buf []byte, reportInterval time.Duration, maxSpeed float64, stat *ChartData) {

	start := time.Now()
	acc := &account{}
	acc.prevTime = start

	for {
		runtime.Gosched()

		if maxSpeed > 0 {
			elapSec := time.Since(acc.prevTime).Seconds()
			if elapSec > 0 {
				mbps := float64(8*(acc.size-acc.prevSize)) / (1000000 * elapSec)
				if mbps > maxSpeed {
					time.Sleep(time.Millisecond)
					continue
				}
			}
		}

		n, errCall := f(buf)
		if errCall != nil {
			log.Printf("workLoop: %s %s: %v", conn, label, errCall)
			break
		}

		acc.update(n, reportInterval, conn, label, cpsLabel, stat)
	}

	acc.average(start, conn, label, cpsLabel)
}

func validateProberConfig(probeInterval, pktInterval string, pktPerProbe int) (time.Duration, time.Duration, int) {
	if len(probeInterval) < 1 || len(pktInterval) < 1 || pktPerProbe < 1 {
		// if the input values are invalid, then return default values
		log.Println("WARNING: Invalid prober configuration. Reset to default values.")
		probeInterval = "3s"
		pktInterval = "500ms"
		pktPerProbe = 3
	}
	if unicode.IsDigit(rune(probeInterval[len(probeInterval)-1])) {
		probeInterval = probeInterval + "s"
	}
	if unicode.IsDigit(rune(pktInterval[len(pktInterval)-1])) {
		pktInterval = pktInterval + "ms"
	}

	// make sure the values make sense
	pktIntvl, _ := time.ParseDuration(pktInterval)
	probeIntvl, _ := time.ParseDuration(probeInterval)
	if pktIntvl.Seconds() * float64(pktPerProbe) > probeIntvl.Seconds() {
		pktIntvl = time.Duration(probeIntvl.Seconds() * 1000 / float64(pktPerProbe))
	}
	return probeIntvl, pktIntvl, pktPerProbe
}
