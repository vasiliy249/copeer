package copeer

import (
	"net"
	"strconv"
	"time"
	"bytes"
	"bufio"
)

func (cop *Copeer) startServerUdp(packetChan chan *rawPacket) OpResult {
	if cop.udpConn != nil {
		cop.Config.Logger.Println("UDP server is already started")
		return OR_Fail
	}

	select {
	case <-cop.stop:
		cop.stop = make(chan struct{})
	default:
	}

	udpAddr, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(int(cop.Config.UdpPort)))
	if err != nil {
		cop.Config.Logger.Println("Error resolving UDP address: ", err.Error())
		return OR_Fail
	}
	cop.udpConn, err = net.ListenUDP("udp", udpAddr)
	if err != nil {
		cop.Config.Logger.Println("Error during open listen port:", err.Error())
	}
	go cop.serveUdp(packetChan)
	cop.Config.Logger.Println("UDP server is started")
	return OR_Success
}

func (cop *Copeer) serveUdp(packetChan chan *rawPacket) {
	for {
		select {
		case <-cop.stop:
			cop.Config.Logger.Println("Stopping udp server.")
			cop.udpConn.Close()
			close(cop.stoppedUdp)
			return
		default:
			cop.udpConn.SetDeadline(time.Now().Add(time.Second * 3))
			buff := make([]byte, 4096)
			n, addr, err := cop.udpConn.ReadFromUDP(buff)
			if err != nil || n == 4096 {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					continue
				}
				cop.Config.Logger.Println("Error during read from client")
				continue
			}
			rawPack := newRawPacket(bufio.NewReader(bytes.NewReader(buff)), addr)
			if rawPack == nil {
				cop.Config.Logger.Println("Error during attempt to interpret copper packet")
				return
			}
			packetChan<-rawPack
		}
	}
}
