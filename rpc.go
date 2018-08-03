package copeer

import (
	"net"
	"bufio"
	"encoding/binary"
	"strconv"
	"fmt"
	"bytes"
)

type messageType byte

const (
	MT_PING        messageType = iota + 1
	MT_STORE
	MT_FIND_VALUE
	MT_FIND_NODE
	MT_NEW_DATA
	MT_GET_DATA
	MT_UPLOAD_DATA
	MT_GET_MASTERS // for testing
)

type rawPacket struct {
	tcpAddr *net.TCPAddr
	udpAddr *net.UDPAddr

	sourceId dhtKey
	msgType  messageType
	masterId dhtKey
	ma       byte
	cookie   dhtKey
	isAck    bool

	content []byte
}

func newRawPacket(rdr *bufio.Reader, addr net.Addr) (ret *rawPacket) {
	if addr.Network() == "udp" {
		protoIdSlice := make([]byte, 5)
		read, err := rdr.Read(protoIdSlice)
		wrongProto := err != nil || read != 5
		if !wrongProto {
			wrongProto = !bytes.Equal(protoIdSlice, protoID)
		}
		if wrongProto {
			return nil
		}
	}

	sourceIdSlice := make([]byte, 20)
	read, err := rdr.Read(sourceIdSlice)
	if err != nil || read != 20 {
		return nil
	}

	sourceTcpPortSlice := make([]byte, 2)
	read, err = rdr.Read(sourceTcpPortSlice)
	if err != nil || read != 2 {
		return nil
	}
	sourceTcpPort := binary.BigEndian.Uint16(sourceTcpPortSlice)
	sourceTcpPortStr := strconv.FormatUint(uint64(sourceTcpPort), 10)

	sourceUdpPortSlice := make([]byte, 2)
	read, err = rdr.Read(sourceUdpPortSlice)
	if err != nil || read != 2 {
		return nil
	}
	sourceUdpPort := binary.BigEndian.Uint16(sourceUdpPortSlice)
	sourceUdpPortStr := strconv.FormatUint(uint64(sourceUdpPort), 10)

	var tcpAddr *net.TCPAddr
	var udpAddr *net.UDPAddr
	var netIP net.IP
	if addr.Network() == "tcp" {
		tcpAddr, err = net.ResolveTCPAddr("tcp", addr.String())
		if err != nil {
			return nil
		}
		netIP = tcpAddr.IP
	} else if addr.Network() == "udp" {
		udpAddr, err = net.ResolveUDPAddr("udp", addr.String())
		if err != nil {
			return nil
		}
		netIP = udpAddr.IP
	}
	tcpAddr, err = net.ResolveTCPAddr("tcp", netIP.String()+":"+sourceTcpPortStr)
	if err != nil {
		return nil
	}
	udpAddr, err = net.ResolveUDPAddr("udp", netIP.String()+":"+sourceUdpPortStr)
	if err != nil {
		return nil
	}

	msgIdSlice := make([]byte, 1)
	read, err = rdr.Read(msgIdSlice)
	if err != nil || read != 1 {
		return nil
	}
	msgType := messageType(msgIdSlice[0])

	masterIdSlice := make([]byte, 20)
	read, err = rdr.Read(masterIdSlice)
	if err != nil || read != 20 {
		return nil
	}

	maSlice := make([]byte, 1)
	read, err = rdr.Read(maSlice)
	if err != nil || read != 1 {
		return nil
	}

	cookieSlice := make([]byte, 20)
	read, err = rdr.Read(cookieSlice)
	if err != nil || read != 20 {
		return nil
	}

	ackSlice := make([]byte, 1)
	read, err = rdr.Read(ackSlice)
	if err != nil || read != 1 {
		return nil
	}

	sizeSlice := make([]byte, 4)
	read, err = rdr.Read(sizeSlice)
	if err != nil || read != 4 {
		return nil
	}
	contentSize := binary.BigEndian.Uint32(sizeSlice)

	var contentSlice []byte
	if contentSize > 0 {
		contentSlice = make([]byte, contentSize)
		read, err = rdr.Read(contentSlice)
		if err != nil || uint32(read) != contentSize {
			return nil
		}
	}

	ret = &rawPacket{
		tcpAddr: tcpAddr,
		udpAddr: udpAddr,
		msgType: msgType,
		content: contentSlice,
		ma:      maSlice[0],
		isAck:   ackSlice[0] == 1,
	}
	copy(ret.sourceId[:], sourceIdSlice)
	copy(ret.masterId[:], masterIdSlice)
	copy(ret.cookie[:], cookieSlice)
	return
}

func sendMsgTcp(addr net.TCPAddr, header []byte, content []byte) bool {
	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		fmt.Println("Error during establish connection with ", addr)
		return false
	}
	defer conn.Close()

	return sendMsg(conn, header, content)
}

func sendMsgUdp(addr net.UDPAddr, header []byte, content []byte) bool {
	conn, err := net.Dial("udp", addr.String())
	if err != nil {
		fmt.Println("Error during establish connection with ", addr)
		return false
	}
	defer conn.Close()

	return sendMsg(conn, header, content)
}

func sendMsg(conn net.Conn, header []byte, content []byte) bool {
	fullSize := len(header) + 4 + len(content)
	fullBuff := make([]byte, 0)
	fullBuff = append(fullBuff, header...)
	sizeSlice := make([]byte, 4)
	if content != nil && len(content) > 0 {
		binary.BigEndian.PutUint32(sizeSlice, uint32(len(content)))
		fullBuff = append(fullBuff, sizeSlice...)
		fullBuff = append(fullBuff, content...)
	} else {
		binary.BigEndian.PutUint32(sizeSlice, uint32(0))
		fullBuff = append(fullBuff, sizeSlice...)
	}

	n, err := conn.Write(fullBuff)
	if err != nil || n != fullSize {
		fmt.Println("Error sending tcp message to ", conn.RemoteAddr())
		return false
	}
	return true
}
