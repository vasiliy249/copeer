package copeer

import (
	"net"
	"strconv"
	"strings"
	"time"
	"bufio"
	"bytes"
)

func (cop *Copeer) startServeTcp(packetChan chan *rawPacket) OpResult {
	if cop.tcpListener != nil {
		cop.Config.Logger.Println("TCP server is already started")
		return OR_Fail
	}

	select {
	case <-cop.stop:
		cop.stop = make(chan struct{})
	default:
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+strconv.FormatUint(uint64(cop.Config.TcpPort), 10))
	if err != nil {
		cop.Config.Logger.Println("Error resolving TCP address: ", err.Error())
		return OR_Fail
	}
	cop.tcpListener, err = net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		cop.Config.Logger.Println("Error openning listen port:", err.Error())
		return OR_Fail
	}
	go cop.serveTcp(packetChan)
	cop.Config.Logger.Println("TCP server is started")
	return OR_Success
}

func (cop *Copeer) serveTcp(packetChan chan *rawPacket) {
	for {
		select {
		case <-cop.stop:
			cop.Config.Logger.Println("Stopping tcp server.")
			for i := 0; i < MAX_TCP_CONNECTIONS; i++ {
				if cop.tcpHandling[i] != nil {
					<-cop.tcpHandling[i]
				}
			}
			close(cop.stoppedTcp)
			return
		default:
			conn, err := cop.tcpListener.Accept()
			if err != nil {
				if !strings.Contains(err.Error(), "use of closed network connection") {
					cop.Config.Logger.Println("Error during accepting connection: ", err.Error())
				}
				continue
			}
			connIndex := cop.getFirstFreeTcpHandleIndex()
			if connIndex == -1 {
				conn.Write([]byte("Maximum number of connections\n"))
				conn.Close()
				cop.Config.Logger.Println("Maximum number of simultaneous connections has been reached")
				time.Sleep(time.Second)
				continue
			}

			cop.tcpHandling[connIndex] = make(chan struct{})
			go cop.handleTcpConn(conn, connIndex, packetChan)
		}
	}
}

func (cop *Copeer) getFirstFreeTcpHandleIndex() int {
	for i, v := range cop.tcpHandling {
		if v == nil {
			return i
		}
	}
	return -1
}

func (cop *Copeer) handleTcpConn(conn net.Conn, index int, packetChan chan *rawPacket) {
	defer cop.closeTcpConn(conn, index)

	connReader := bufio.NewReader(conn)
	protoIdSlice := make([]byte, 5)
	read, err := connReader.Read(protoIdSlice)
	wrongProto := err != nil || read != 5
	if !wrongProto {
		wrongProto = !bytes.Equal(protoIdSlice, protoID)
	}
	if wrongProto {
		cop.Config.Logger.Println("Recieved package with wrong proto ID")
		return
	}
	rawPack := newRawPacket(connReader, conn.RemoteAddr())
	if rawPack == nil {
		cop.Config.Logger.Println("Error during attempt to interpret copper packet")
		return
	}
	packetChan <- rawPack
}

func (cop *Copeer) closeTcpConn(conn net.Conn, index int) {
	conn.Close()
	close(cop.tcpHandling[index])
	cop.tcpHandling[index] = nil
}

// Get preferred outbound ip of this machine
func GetOutboundIP() net.IP {
	iface, err := net.InterfaceByName(NetIfaceName)
	if err != nil {
		return nil
	}
	addrs, err := iface.Addrs()
	if err != nil {
		return nil
	}
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		if ipnet.IP.To4() != nil {
			return ipnet.IP
		}
	}
	return nil
}
