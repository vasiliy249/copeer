package copeer

import (
	"log"
	"os"
	"net"
	"encoding/binary"
	"strconv"
	"math/rand"
	"time"
	"fmt"
)

const (
	MAX_TCP_CONNECTIONS = 8
)

type OpResult int

const (
	OR_Success OpResult = iota
	OR_Fail
)

var protoID = []byte{0x43, 0x4F, 0x50, 0x45, 0x52}

var NetIfaceName = ""

type Config struct {
	TcpPort   uint16
	UdpPort   uint16
	Bootstrap string
	K         int
	Logger    *log.Logger

	BsType  bool
	Masters []string

	MasterType bool

	MasterAddrStr string
}

func NewConfig() *Config {
	return &Config{
		TcpPort:       9797,
		UdpPort:       9797,
		K:             20,
		Bootstrap:     "192.168.240.1:9797",
		Logger:        log.New(os.Stdout, "===Copeer Log: ", 0),
		BsType:        false,
		MasterType:    false,
		MasterAddrStr: "192.168.240.1:9797",
	}
}

func (cfg *Config) SetBs() {
	cfg.BsType = true
	cfg.Masters = []string{"192.168.240.1:9797"}
	return
}

func (cfg *Config) SetMaster() {
	cfg.MasterType = true
	return
}

type Copeer struct {
	NodeId  dhtKey
	Config  *Config
	rtState *routingState
	ma      byte

	estimatedData dhtKey

	tcpListener *net.TCPListener
	udpConn     *net.UDPConn
	stop        chan struct{}

	stoppedTcp chan struct{}
	stoppedUdp chan struct{}

	tcpHandling [MAX_TCP_CONNECTIONS]chan struct{}

	bootstrapStay     bool
	masterFindingStay bool
}

func NewCopeer(config *Config) (ret *Copeer) {
	if config == nil {
		config = NewConfig()
	}

	ret = &Copeer{
		Config:        config,
		ma:            0,
		estimatedData: NewZeroDhtKey(),
		NodeId:        NewRandomDhtKeyHash(GetOutboundIP()),
		rtState:       newRoutingState(config.K),
		stop:          make(chan struct{}),
		stoppedUdp:    make(chan struct{}),
		stoppedTcp:    make(chan struct{}),
		bootstrapStay: false,
		masterFindingStay: false,
	}
	if !ret.Config.MasterType {
		// for testing purposes
		dataKey := NewRandomDhtKeyHash(GetOutboundIP())
		ret.rtState.storeValue(NewRandomDhtKeyHash(GetOutboundIP()), &nodeOrValue{node: false, data: dataKey[:]})
	}
	return
}

func (cop *Copeer) Start() {
	rand.Seed(time.Now().UnixNano()) //?
	cop.Config.Logger.Printf("Starting with ID: %s", cop.NodeId.String())
	cop.Config.Logger.Println("Starting as master: ", cop.Config.MasterType)
	if !cop.Config.MasterType {
		for key, val := range cop.rtState.store {
			if val.node == false {
				cop.Config.Logger.Println("Starting with testing dataKey:", key, "and value: ", val.data)
				break
			}
		}
	}
	cop.Config.Logger.Println("Launching the servers...")
	socketChan := make(chan *rawPacket)
	resTcp := cop.startServeTcp(socketChan)
	resUdp := cop.startServerUdp(socketChan)

	if resTcp != OR_Success {
		cop.Config.Logger.Println("Failed to start TCP server, shutdown...")
		cop.tcpListener = nil
		if resUdp == OR_Success {
			cop.Stop()
		}
		return
	}
	if resUdp != OR_Success {
		cop.Stop()
		cop.Config.Logger.Println("Failed to start UDP server, shutdown...")
		cop.udpConn = nil
		if resTcp == OR_Success {
			cop.Stop()
		}
		return
	}

	// TODO:rewrite bootstrapping
	if !cop.Config.BsType {
		cop.Config.Logger.Println("Sending ping to bootstrap node...")
		cop.bootstrapStay = true
		cop.PingNode(cop.Config.Bootstrap)

	}
	if !cop.Config.MasterType {
		cop.Config.Logger.Println("Sending ping to master node...")
		cop.masterFindingStay = true
		cop.PingNode(cop.Config.MasterAddrStr)
	}

	for {
		select {
		case <-cop.stop:
			return
		case rawPack := <-socketChan:
			cop.processRawPack(rawPack)
		}
	}
}

func (cop *Copeer) Stop() {
	close(cop.stop)
	if cop.tcpListener != nil {
		cop.tcpListener.Close()
		<-cop.stoppedTcp
		cop.tcpListener = nil
		cop.Config.Logger.Println("TCP server is stopped")
	}
	if cop.udpConn != nil {
		<-cop.stoppedUdp
		cop.udpConn = nil
		cop.Config.Logger.Println("UDP server is stopped")
	}
}

// for testing purposes
func (cop *Copeer) Publish() {
	if cop.Config.MasterType {
		return
	}
	var keyToNotify dhtKey
	for key, val := range cop.rtState.store {
		if val.node == false {
			keyToNotify = key
		}
	}
	cop.Config.Logger.Println("Publishing data with key: ", keyToNotify.String())

	if cop.callPing(cop.rtState.master) {
		cnt := cop.rtState.getClosestContact(keyToNotify, cop.NodeId, true)
		if cnt == nil {
			// kbucket empty?
			return
		}
		cop.callStore(cnt, keyToNotify)

		time.Sleep(time.Second) // for testing

		if cop.notifyNewData(keyToNotify) {
			return
		}
	}

	cop.Config.Logger.Println("There is no connection with master, uploading to another node...")

	for i, _ := range cop.rtState.kBuckets {
		if len(cop.rtState.kBuckets[i]) > 0 {
			for j, _ := range cop.rtState.kBuckets[i] {
				if !cop.rtState.kBuckets[i][j].master.Equals(NewZeroDhtKey()) {
					// upload to first non-master node
					cop.callUploadData(cop.rtState.kBuckets[i][j], keyToNotify)
				}
			}
		}
	}
}

func (cop *Copeer) ShowRouteState() {
	fmt.Println(cop.rtState.String())
}

func (cop *Copeer) preparePackHeader(tcp bool, msgType messageType, mc dhtKey, ack bool) []byte {
	ret := make([]byte, 0)
	ret = append(ret, protoID...)
	ret = append(ret, cop.NodeId[:]...)

	portSlice := make([]byte, 2)
	binary.BigEndian.PutUint16(portSlice, cop.Config.TcpPort)
	ret = append(ret, portSlice...)
	binary.BigEndian.PutUint16(portSlice, cop.Config.UdpPort)
	ret = append(ret, portSlice...)

	ret = append(ret, byte(msgType))
	if cop.Config.MasterType {
		zeroKey := NewZeroDhtKey()
		ret = append(ret, zeroKey[:]...)
	} else {
		ret = append(ret, cop.rtState.master.id[:]...)
	}
	ret = append(ret, cop.ma)
	ret = append(ret, mc[:]...)
	var ackByte byte = 1
	if !ack {
		ackByte = 0
	}
	ret = append(ret, ackByte)
	return ret
}

func (cop *Copeer) selfContact() *contact {
	tcpAddr, err := net.ResolveTCPAddr("tcp", GetOutboundIP().String() + ":"+
		strconv.FormatUint(uint64(cop.Config.TcpPort), 10))
	if err != nil {
		return nil
	}
	udpAddr, err := net.ResolveUDPAddr("udp", GetOutboundIP().String() + ":"+
		strconv.FormatUint(uint64(cop.Config.UdpPort), 10))
	if err != nil {
		return nil
	}

	return &contact{
		id:           cop.NodeId,
		tcpAddr:      *tcpAddr,
		udpAddr:      *udpAddr,
		master:       cop.rtState.master.id,
		masterAccess: cop.ma,
	}
}

func (cop *Copeer) processRawPack(rawPack *rawPacket) {
	cop.Config.Logger.Println("Process copeer packet...")
	cnt := newContactFromPack(rawPack)
	cnt = cop.rtState.addContact(cnt, cop.NodeId) // get or add
	if cnt == nil {
		//TODO: deal with it
		return
	}

	//spec.processing
	switch rawPack.msgType {
	case MT_GET_DATA:
		cop.processGetData(rawPack, cnt)
	case MT_PING:
		cop.processPing(rawPack, cnt)
	case MT_STORE:
		cop.processStore(rawPack, cnt)
	case MT_NEW_DATA:
		cop.processNewData(rawPack)
	case MT_FIND_NODE:
		cop.processFindNode(rawPack, cnt)
	case MT_FIND_VALUE:
		cop.processFindValue(rawPack, cnt)
	case MT_UPLOAD_DATA:
		cop.processUploadData(rawPack, cnt)
	}
	return
}

func (cop *Copeer) processGetData(rawPack *rawPacket, cnt *contact) {
	if !rawPack.isAck {
		if cop.Config.MasterType {
			// request get_data, but we are master ?!
			return
		}
		// Master is requesting Data from us
		if len(rawPack.content) != keyLenBytes {
			cop.Config.Logger.Println("Wrong payload in GET_DATA!")
			return
		}
		buff := cop.preparePackHeader(true, MT_GET_DATA, rawPack.cookie, true)

		contentBuff := make([]byte, 0)

		var requestedDataKey dhtKey
		copy(requestedDataKey[:], rawPack.content)
		storedVal := cop.rtState.getStoredValue(requestedDataKey)
		if storedVal != nil && storedVal.node == false {
			// found in local store!
			contentBuff = append(contentBuff, 1)
			contentBuff = append(contentBuff, storedVal.data...) // value
		} else {
			contentBuff = append(contentBuff, 0) // not found
		}
		sendMsgTcp(cop.rtState.master.tcpAddr, buff, contentBuff)
	} else {
		if !cop.Config.MasterType {
			// acknowledge, but we aren't master ?!
			return
		}
		// We are master, agent replied to us.
		if cnt.isQueryExists(rawPack.cookie) {
			cnt.endQuery(rawPack.cookie)
			if len(rawPack.content) < 1 {
				cop.Config.Logger.Println("Wrong payload in GET_DATA!")
				return
			}
			if rawPack.content[0] != 1 {
				// val not found in node's local store
				// TODO: What behavior is next?
				return
			}
			if len(rawPack.content) < keyLenBytes+1 {
				cop.Config.Logger.Println("Wrong payload in GET_DATA!")
				return
			}
			newData := rawPack.content[1 : keyLenBytes+1]
			var newDataStr string
			for i, _ := range newData {
				newDataStr += strconv.FormatUint(uint64(newData[i]), 10) + "_"
			}
			cop.Config.Logger.Println("Received new data! ", newDataStr)
		}
	}
}

func (cop *Copeer) processPing(rawPack *rawPacket, cnt *contact) {
	if rawPack.isAck {
		// we asked, peer responded
		if cnt != nil {
			if cnt.isQueryExists(rawPack.cookie) {
				cop.Config.Logger.Println("Received answer on PING! ")
				cnt.endQuery(rawPack.cookie)
				// TODO: refresh peer state
			}
			if cop.bootstrapStay && cnt.udpAddr.String() == cop.Config.Bootstrap {
				cop.callFindNode(cnt, cop.NodeId)
				cop.bootstrapStay = false
				return
			}
			if cop.masterFindingStay && cnt.udpAddr.String() == cop.Config.MasterAddrStr {
				cop.rtState.master = cnt
				cop.masterFindingStay = false
				return
			}
		}
	} else {
		// we are asked
		buff := cop.preparePackHeader(false, MT_PING, rawPack.cookie, true)
		sendMsgUdp(cnt.udpAddr, buff, nil)
	}
}

func (cop *Copeer) processStore(rawPack *rawPacket, cnt *contact) {
	if rawPack.isAck {
		// we sent a STORE request, peer has answered.
		if cnt.isQueryExists(rawPack.cookie) {
			cop.Config.Logger.Println("Received answer on STORE!")
			cnt.endQuery(rawPack.cookie)
		}
	} else {
		// we are asked to STORE smth.
		if len(rawPack.content) != keyLenBytes {
			cop.Config.Logger.Println("Wrong payload in STORE!")
			return
		}
		var keyToStore dhtKey
		copy(keyToStore[:], rawPack.content)
		cop.rtState.storeValue(keyToStore, &nodeOrValue{node: true, data: cnt.toBytes()}) // storing.

		// prepare answer
		buff := cop.preparePackHeader(false, MT_STORE, rawPack.cookie, true)
		sendMsgUdp(cnt.udpAddr, buff, nil)
	}
}

func (cop *Copeer) processNewData(rawPack *rawPacket) {
	if !cop.Config.MasterType {
		return // smth went wrong...
	}
	// we are master and some agent notified us that there was new data to transfer
	if len(rawPack.content) != keyLenBytes {
		cop.Config.Logger.Println("Wrong payload in NEW_DATA!")
		return
	}
	if rawPack.masterId != cop.NodeId {
		// just extract metainfo and etc...
		return
	}

	// our agent! Initiate data collection
	copy(cop.estimatedData[:], rawPack.content)
	storedVal := cop.rtState.getStoredValue(cop.estimatedData)
	if storedVal != nil {
		// two variants: value already exists in local store
		//               node that stores value in local store
		if storedVal.node == false {
			// already exists in local store
			//TODO: answer
			return
		} else {
			cnt := newContactFromContent(storedVal.data)
			cnt = cop.rtState.addContact(cnt, cop.NodeId)
			if cnt != nil {
				// we were lucky.
				cop.callGetData(cnt, cop.estimatedData)
				return
			}
		}
	}

	cnt := cop.rtState.getClosestContact(cop.estimatedData, cop.NodeId, true)
	if cnt == nil {
		return // buckets is empty
	}
	cop.callFindValue(cnt, cop.estimatedData)
}

func (cop *Copeer) processFindNode(rawPack *rawPacket, targetCnt *contact) {
	if rawPack.isAck {
		// node sent its nodes
		if targetCnt.isQueryExists(rawPack.cookie) {
			cop.Config.Logger.Println("Received answer on FIND_NODE!")
			targetCnt.endQuery(rawPack.cookie)
		} else {
			return
		}
		if len(rawPack.content) < 0 {
			cop.Config.Logger.Println("Wrong payload in answer to FIND_NODE!")
			return
		}
		if (len(rawPack.content))%peerContactSize != 0 {
			cop.Config.Logger.Println("Wrong payload in answer to FIND_NODE!")
			return
		}
		for i := 0; i < len(rawPack.content); i += peerContactSize {
			cnt := newContactFromContent(rawPack.content[i : i+peerContactSize])
			if cnt != nil {
				cop.rtState.addContact(cnt, cop.NodeId)
			}
		}
	} else {
		// node asked us for nodes which closest to key
		if len(rawPack.content) != keyLenBytes {
			cop.Config.Logger.Println("Wrong payload in FIND_NODE!")
			return
		}

		buff := cop.preparePackHeader(false, MT_FIND_NODE, rawPack.cookie, true)

		var requestedKey dhtKey
		copy(requestedKey[:], rawPack.content)
		cnt := cop.rtState.getClosestContact(requestedKey, cop.NodeId, false)
		if cnt == nil {
			cnt = cop.selfContact()
		}
		contentBuff := make([]byte, 0)
		contentBuff = append(contentBuff, cnt.toBytes()...)

		sendMsgUdp(targetCnt.udpAddr, buff, contentBuff)
	}
}

func (cop *Copeer) processFindValue(rawPack *rawPacket, targetCnt *contact) {
	if rawPack.isAck {
		// node sent its closest node or node on which data is stored
		if targetCnt.isQueryExists(rawPack.cookie) {
			cop.Config.Logger.Println("Received answer on FIND_VALUE!")
			targetCnt.endQuery(rawPack.cookie)
		} else {
			return
		}
		if len(rawPack.content) < keyLenBytes+1+peerContactSize {
			cop.Config.Logger.Println("Wrong payload in FIND_VALUE!")
			return
		}
		var requestedKey dhtKey
		copy(requestedKey[:], rawPack.content[:keyLenBytes])
		exactNode := rawPack.content[keyLenBytes] == 1
		cnt := newContactFromContent(rawPack.content[keyLenBytes+1 : keyLenBytes+1+peerContactSize])
		cnt = cop.rtState.addContact(cnt, cop.NodeId)
		if cnt == nil {
			// malformed packet
			return
		}
		if exactNode {
			// We found the node on which the data is stored. Check and GET_DATA
			if cop.estimatedData.Equals(requestedKey) {
				cop.callGetData(cnt, cop.estimatedData)
			}
		} else {
			// Node returned to us nodes closest to the requested key
			cop.callFindValue(cnt, requestedKey)
		}
	} else {
		// node asked us for value or nodes which closest to key
		if len(rawPack.content) != keyLenBytes {
			cop.Config.Logger.Println("Wrong payload in FIND_VALUE!")
			return
		}
		buff := cop.preparePackHeader(false, MT_FIND_VALUE, rawPack.cookie, true)

		contentBuff := make([]byte, 0)
		var requestedKey dhtKey
		copy(requestedKey[:], rawPack.content)
		contentBuff = append(contentBuff, requestedKey[:]...) // repeat req key in answer packet

		storedVal := cop.rtState.getStoredValue(requestedKey)
		if storedVal != nil {
			contentBuff = append(contentBuff, 1) // exact node
			// value found in local store (node or value)
			if storedVal.node {
				contentBuff = append(contentBuff, storedVal.data...)
			} else {
				contentBuff = append(contentBuff, cop.selfContact().toBytes()...)
			}
		} else {
			contentBuff = append(contentBuff, 0) // closest node
			cnt := cop.rtState.getClosestContact(requestedKey, cop.NodeId, true)
			if cnt == nil {
				// TODO: kBuckets is empty!?
				return
			}
			contentBuff = append(contentBuff, cnt.toBytes()...)
		}
		sendMsgUdp(targetCnt.udpAddr, buff, contentBuff)
	}
}

func (cop *Copeer) processUploadData(rawPack *rawPacket, targetCnt *contact) {
	if rawPack.isAck {
		// peer successfully received data and sent acknowledge
		if targetCnt.isQueryExists(rawPack.cookie) {
			cop.Config.Logger.Println("Received answer on UPLOAD_DATA!")
			targetCnt.endQuery(rawPack.cookie)
		}
		return
	} else {
		// peer wants us to receive data
		if len(rawPack.content) < keyLenBytes+1 {
			cop.Config.Logger.Println("Wrong payload in UPLOAD_DATA!")
			return
		}
		var reqDataKey dhtKey
		copy(reqDataKey[:], rawPack.content[:keyLenBytes])
		reqData := rawPack.content[keyLenBytes:]
		cop.rtState.storeValue(reqDataKey, &nodeOrValue{node: false, data: reqData})
		cop.Config.Logger.Println("Successfully received UPLOAD_DATA with key:", reqDataKey.String())

		buff := cop.preparePackHeader(true, MT_UPLOAD_DATA, rawPack.cookie, true)
		sendMsgTcp(targetCnt.tcpAddr, buff, nil)

		// STORE & NEW_DATA
		cop.callStore(cop.rtState.getClosestContact(reqDataKey, cop.NodeId, true), reqDataKey)
		time.Sleep(time.Second)
		cop.notifyNewData(reqDataKey)
	}
}

func (cop *Copeer) PingNode(udpAddrStr string) {
	udpAddr, err := net.ResolveUDPAddr("udp", udpAddrStr)
	if err != nil {
		cop.Config.Logger.Println("Wrong udp address for PingNode!")
		return
	}
	cnt := &contact{cookies: make(map[dhtKey]bool), udpAddr: *udpAddr}
	cop.callPing(cnt)
}

func (cop *Copeer) callPing(cnt *contact) bool {
	if cnt == nil {
		return true
	}
	magicCookie := NewRandomDhtKeyHash(GetOutboundIP())
	cnt.addQuery(magicCookie)
	buff := cop.preparePackHeader(false, MT_PING, magicCookie, false)
	return sendMsgUdp(cnt.udpAddr, buff, nil)
}

func (cop *Copeer) callStore(cnt *contact, keyToStore dhtKey) {
	if cnt == nil {
		return
	}
	magicCookie := NewRandomDhtKeyHash(GetOutboundIP())
	cnt.addQuery(magicCookie)
	buff := cop.preparePackHeader(false, MT_STORE, magicCookie, false)

	contentBuff := make([]byte, 0)
	contentBuff = append(contentBuff, keyToStore[:]...)
	sendMsgUdp(cnt.udpAddr, buff, contentBuff)
}

func (cop *Copeer) callFindValue(cnt *contact, valueToFind dhtKey) {
	if cnt == nil {
		return
	}
	magicCookie := NewRandomDhtKeyHash(GetOutboundIP())
	cnt.addQuery(magicCookie)
	buff := cop.preparePackHeader(false, MT_FIND_VALUE, magicCookie, false)

	contentBuff := make([]byte, 0)
	contentBuff = append(contentBuff, valueToFind[:]...)
	sendMsgUdp(cnt.udpAddr, buff, contentBuff)
}

func (cop *Copeer) callFindNode(cnt *contact, nodeToFind dhtKey) {
	if cnt == nil {
		return
	}
	magicCookie := NewRandomDhtKeyHash(GetOutboundIP())
	cnt.addQuery(magicCookie)
	buff := cop.preparePackHeader(false, MT_FIND_NODE, magicCookie, false)

	contentBuff := make([]byte, 0)
	contentBuff = append(contentBuff, nodeToFind[:]...)
	sendMsgUdp(cnt.udpAddr, buff, contentBuff)
}

func (cop *Copeer) notifyNewData(newDataKey dhtKey) bool {
	if cop.Config.MasterType {
		return true
	}
	return cop.callNewData(cop.rtState.master, newDataKey)
}

func (cop *Copeer) callNewData(cnt *contact, newDataKey dhtKey) bool {
	if cnt == nil {
		return true //!
	}
	magicCookie := NewZeroDhtKey()
	cnt.addQuery(magicCookie)
	buff := cop.preparePackHeader(false, MT_NEW_DATA, magicCookie, false)

	contentBuff := make([]byte, 0)
	contentBuff = append(contentBuff, newDataKey[:]...)
	return sendMsgTcp(cnt.tcpAddr, buff, contentBuff)
}

func (cop *Copeer) callGetData(cnt *contact, dataToGet dhtKey) {
	if cnt == nil {
		return
	}
	magicCookie := NewRandomDhtKeyHash(GetOutboundIP())
	cnt.addQuery(magicCookie)
	buff := cop.preparePackHeader(false, MT_GET_DATA, magicCookie, false)

	contentBuff := make([]byte, 0)
	contentBuff = append(contentBuff, dataToGet[:]...)
	sendMsgTcp(cnt.tcpAddr, buff, contentBuff)
}

func (cop *Copeer) callUploadData(cnt *contact, key dhtKey) {
	if cnt == nil {
		return
	}
	storedVal := cop.rtState.getStoredValue(key)
	if storedVal == nil || storedVal.node {
		return
	}
	magicCookie := NewRandomDhtKeyHash(GetOutboundIP())
	cnt.addQuery(magicCookie)
	buff := cop.preparePackHeader(false, MT_UPLOAD_DATA, magicCookie, false)

	contentBuff := make([]byte, 0)
	contentBuff = append(contentBuff, key[:]...)
	contentBuff = append(contentBuff, storedVal.data...)
	sendMsgTcp(cnt.tcpAddr, buff, contentBuff)
}
