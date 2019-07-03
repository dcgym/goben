package main

import (
	"bytes"
	"crypto/tls"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime"
	"sync"
	"time"
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
					spawnClient(app, &wg, conn, i, app.connections, h)
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
			spawnClient(app, &wg, conn, i, app.connections, h)
		}
	}

	wg.Wait()
}

func spawnClient(app *config, wg *sync.WaitGroup, conn net.Conn, c, connections int, targetHost string) {
	wg.Add(1)
	if app.csv != "" {
		filename := fmt.Sprintf(app.csv, c, conn.RemoteAddr())
		log.Printf("Creating CVS file: %s", filename)
		errExport := initCSV(filename)
		if errExport != nil {
			log.Printf("handleConnectionClient: export CSV: %s: %v", filename, errExport)
		}
	}

	go handleConnectionClient(app, wg, conn, c, connections)
	// turn off rtt measurement when measuring fixed flow completion time
	/*	if app.opt.totalFlow == 0 {
			wg.Add(1)
			go handleMeasurement(app, targetHost, wg, c)
		}*/
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
	Input  			ChartData
	Output 			ChartData
	completionTime	time.Duration
}

func sendOptions(app *config, conn net.Conn) error {
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

	log.Printf("handleConnectionClient: options sent: %v", opt)

	// receive ack
	// FIXME WRITEME server does not send ack for UDP
	if !app.udp {
		var a ack
		if errAck := ackRecv(app.udp, conn, &a); errAck != nil {
			log.Printf("handleConnectionClient: receiving ack: %v", errAck)
			return errAck
		}
		log.Printf("handleConnectionClient: Received ACK.")
	}
	return nil
}

func handleConnectionClient(app *config, wg *sync.WaitGroup, conn net.Conn, c, connections int) {
	defer wg.Done()

	info := ExportInfo{
		Input:  ChartData{},
		Output: ChartData{},
	}
	log.Printf("handleConnectionClient: starting %s %d/%d %v", c, connections, conn.RemoteAddr())
	// send options
	if errOpt := sendOptions(app, conn); errOpt != nil {
		return
	}
	var input *ChartData
	var output *ChartData
	opt := app.opt
	if (app.csv != "" && app.totalDuration != "inf" && opt.totalFlow == 0) || app.export != "" {
		input = &info.Input
		output = &info.Output
	}

	for start := time.Now(); time.Since(start) < app.opt.TotalDuration; {

		doneReader := make(chan time.Duration)
		doneWriter := make(chan time.Duration)
		if !app.passiveClient {
			go clientWriter(conn, c, connections, doneWriter, opt, output)
		}
		go clientReader(conn, c, connections, doneReader, opt, input)

		if !opt.PassiveServer {
			select {
				case <-doneReader: // wait reader exit:
				case <-time.After(app.opt.TotalDuration):
					log.Printf("Flow Time Limit exceeded!\n")
			}
		}
		if !app.passiveClient {
			select {
				case info.completionTime = <-doneWriter: // wait writer exit:
				case <-time.After(app.opt.TotalDuration):
					log.Printf("Flow Time Limit exceeded!\n")
			}
		}
		generateDeliverables(app, c, conn, &info)
		log.Printf("handleConnectionClient: closing: %d/%d %v", c, connections, conn.RemoteAddr())
	}
	// clean up client
	conn.Close() // force reader/writer to quit
}

// init and start Prober
func handleMeasurement(app *config, targetHost string, wg *sync.WaitGroup, connIndex int) {
		defer wg.Done()

		proto := "ip4:icmp" // currently we only handel ipv4 tcp
		source, er := os.Hostname()
		if er != nil {
			log.Panicf("Cannot get the host machine hostname. %v", er.Error())
		}
		proberConfig := ProberConfig {
			proto,
			source,		 	// default is the local machine's external IP
			targetHost,
			app.csv,
			connIndex,
		}
		var prober Prober
		err := prober.Init(proberConfig)
		if err != nil {
			log.Panicf("Cannot initialize the prober! %v \n", err.Error())
		}

		go prober.Start()

		startTicker(app.opt.TotalDuration)

		if app.csv != "" {
			csvErr := closeCSV(prober.result, prober.file)
			if csvErr != nil {
				log.Panicf("Cannot close the csv file: %v \n", csvErr.Error())
			}
		}
}

func clientReader(conn net.Conn, c, connections int, done chan time.Duration, opt options, stat *ChartData) {
	log.Printf("clientReader: starting: %d/%d %v", c, connections, conn.RemoteAddr())

	connIndex := fmt.Sprintf("%d/%d", c, connections)

	buf := make([]byte, opt.ReadSize)

	workLoop(connIndex, "clientReader", "rcv/s", conn.Read, buf, opt.ReportInterval, 0, stat, done, opt.totalFlow)

	close(done)

	log.Printf("clientReader: exiting: %d/%d %v", c, connections, conn.RemoteAddr())
}

func clientWriter(conn net.Conn, c, connections int, done chan time.Duration, opt options, stat *ChartData) {
	log.Printf("clientWriter: starting: %d/%d %v", c, connections, conn.RemoteAddr())

	connIndex := fmt.Sprintf("%d/%d", c, connections)

	buf := randBuf(opt.WriteSize)

	workLoop(connIndex, "clientWriter", "snd/s", conn.Write, buf, opt.ReportInterval, opt.MaxSpeed, stat, done, opt.totalFlow)

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

func workLoop(conn, label, cpsLabel string, f call, buf []byte, reportInterval time.Duration, maxSpeed float64, stat *ChartData, done chan time.Duration, totalFlow uint64) {

	start := time.Now()
	acc := &account{}
	acc.prevTime = start
	totalBytesSoFar := 0

	for (uint64(totalBytesSoFar) < totalFlow || totalFlow == 0) {
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
		totalBytesSoFar = totalBytesSoFar + n

		if errCall != nil {
			log.Printf("workLoop: %s %s: %v", conn, label, errCall)
			break
		}
		acc.update(n, reportInterval, conn, label, cpsLabel, stat)
	}
	completionTime := time.Since(start)
	acc.average(start, conn, label, cpsLabel)
	if done != nil {
		done <- completionTime // signal that the routine has completed
	}
	return
}

// Control totalDuration: start the timer that will tick the given duration time
func startTicker(duration time.Duration) {
	tickerPeriod := time.NewTimer(duration)

	<-tickerPeriod.C
	log.Printf("Connection lasts: %v timer", duration)

	tickerPeriod.Stop()
}

func generateDeliverables(app *config, c int, conn net.Conn, info *ExportInfo) {
	if app.csv != "" && app.totalDuration != "inf" {
		filename := fmt.Sprintf(app.csv, c, conn.RemoteAddr())
		log.Printf("append CSV test results to: %s", filename)
		errExport := appendCSV(filename, info)
		if errExport != nil {
			log.Printf("handleConnectionClient: export CSV: %s: %v", filename, errExport)
		}
	}

	if app.export != "" {
		filename := fmt.Sprintf(app.export, c, conn.RemoteAddr())
		log.Printf("exporting YAML test results to: %s", filename)
		errExport := export(filename, info)
		if errExport != nil {
			log.Printf("handleConnectionClient: export YAML: %s: %v", filename, errExport)
		}
	}
}
