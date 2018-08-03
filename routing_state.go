package copeer

import (
	"net"
	"strconv"
	"encoding/binary"
	"fmt"
)

const peerContactSize = 49

type contact struct {
	id           dhtKey
	tcpAddr      net.TCPAddr
	udpAddr      net.UDPAddr
	cookies      map[dhtKey]bool
	master       dhtKey
	masterAccess byte
}

func newContact(tcpAd net.TCPAddr, udpAd net.UDPAddr, nodeId dhtKey, master dhtKey, ma byte) (ret *contact) {
	ret = &contact{
		id:           nodeId,
		tcpAddr:      tcpAd,
		udpAddr:      udpAd,
		cookies:      make(map[dhtKey]bool),
		masterAccess: ma,
		master:       master,
	}
	return
}

func newContactFromPack(rawPack *rawPacket) *contact {
	return newContact(*rawPack.tcpAddr, *rawPack.udpAddr, rawPack.sourceId, rawPack.masterId, rawPack.ma)
}

func newContactFromContent(buff []byte) *contact {
	if len(buff) != peerContactSize {
		return nil
	}
	key := buff[:keyLenBytes]
	var addressStr = strconv.FormatUint(uint64(buff[20]), 10)
	for i := 21; i < 24; i++ {
		addressStr += "." + strconv.FormatUint(uint64(buff[i]), 10)
	}
	var portTcpSlice = buff[24:26]
	portTcp := binary.BigEndian.Uint16(portTcpSlice)
	var portUdpSlice = buff[26:28]
	portUdp := binary.BigEndian.Uint16(portUdpSlice)
	tcpAddr, err := net.ResolveTCPAddr("tcp", addressStr+":"+strconv.Itoa(int(portTcp)))
	if err != nil {
		return nil
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addressStr+":"+strconv.Itoa(int(portUdp)))
	if err != nil {
		return nil
	}

	ret := &contact{}
	copy(ret.id[:], key)
	ret.udpAddr = *udpAddr
	ret.tcpAddr = *tcpAddr
	copy(ret.master[:], buff[28:48])
	ret.masterAccess = buff[48]
	ret.cookies = make(map[dhtKey]bool)
	return ret
}

func (ct *contact) addQuery(qKey dhtKey) {
	ct.cookies[qKey] = true
}

func (ct *contact) endQuery(qKey dhtKey) {
	delete(ct.cookies, qKey)
}

func (ct *contact) isQueryExists(qKey dhtKey) bool {
	_, ok := ct.cookies[qKey]
	return ok
}

func (ct *contact) toBytes() (ret []byte) {
	ret = make([]byte, 0)
	ret = append(ret, ct.id[:]...)
	ret = append(ret, ct.udpAddr.IP.To4()...)
	portSlice := make([]byte, 2)
	binary.BigEndian.PutUint16(portSlice, uint16(ct.tcpAddr.Port))
	ret = append(ret, portSlice...)
	binary.BigEndian.PutUint16(portSlice, uint16(ct.udpAddr.Port))
	ret = append(ret, portSlice...)
	ret = append(ret, ct.master[:]...)
	ret = append(ret, ct.masterAccess)
	return
}

func (ct *contact) String() string {
	ret := "Contact ID: " + ct.id.String() + "\n" +
		"TCP address: " + ct.tcpAddr.String() + "\n" +
		"UDP address: " + ct.udpAddr.String() + "\n" +
		"cookies count: " + strconv.Itoa(len(ct.cookies)) + "\n" +
		"master: " + ct.master.String() + "\n" +
		"master access:" + strconv.Itoa(int(ct.masterAccess)) + "\n"
	return ret
}

func (ct *contact) Update(other *contact) {
	if ct.id != other.id {
		return
	}
	ct.masterAccess = other.masterAccess
	ct.master = other.master
	ct.udpAddr = other.udpAddr
	ct.tcpAddr = other.tcpAddr
}

type nodeOrValue struct {
	node bool
	data []byte
}

type routingState struct {
	kBuckets        [keySpaceDim][]*contact
	neighborhoodSet []*contact
	masterSet       []*contact
	k               int
	master          *contact
	store           map[dhtKey]*nodeOrValue
}

func (rs *routingState) String() string {
	var ret string
	ret += "*** ROUTING STATE INFO ***\n\n"
	ret += "kBuckets content:" + "\n"
	for i, _ := range rs.kBuckets {
		if len(rs.kBuckets[i]) > 0 {
			for j, _ := range rs.kBuckets[i] {
				ret += rs.kBuckets[i][j].String() + "\n"
			}
		}
	}
	ret += "\nmaster:\n"
	if rs.master != nil {
		ret += rs.master.String() + "\n"
	} else {
		ret += "no info.\n"
	}
	return ret
}

func (rs *routingState) getContact(key dhtKey) *contact {
	for i, _ := range rs.kBuckets {
		for _, val := range rs.kBuckets[i] {
			if val.id == key {
				return val
			}
		}
	}
	return nil
}

func (rs *routingState) getClosestContact(key dhtKey, self dhtKey, allowMatch bool) *contact {
	kBuckNum := rs.getBucketNum(key, self)
	cnt := rs.getClosestContactInBucket(key, kBuckNum)
	if cnt != nil && (allowMatch || cnt.id != key) {
		return cnt
	}

	// appropriate kBucket is empty :( move to the periphery.
	var limit = kBuckNum
	if (keySpaceDim - kBuckNum) > limit {
		limit = keySpaceDim - kBuckNum
	}
	for i := 0; i < limit+1; i++ {
		cnt := rs.getClosestContactInBucket(key, kBuckNum-i)
		if cnt != nil && (allowMatch || cnt.id != key) {
			return cnt
		}
		cnt = rs.getClosestContactInBucket(key, kBuckNum+i)
		if cnt != nil && (allowMatch || cnt.id != key) {
			return cnt
		}
	}
	return nil
}

func (rs *routingState) getClosestContactInBucket(key dhtKey, kBuckNum int) (ret *contact) {
	if kBuckNum >= 0 && kBuckNum < keySpaceDim && len(rs.kBuckets[kBuckNum]) != 0 {
		minDist := rs.kBuckets[kBuckNum][0].id.xor(key)
		minIdx := 0
		for i, _ := range rs.kBuckets[kBuckNum] {
			if i == 0 {
				continue
			}
			tmpDist := rs.kBuckets[kBuckNum][i].id.xor(key)
			if tmpDist.Less(minDist) {
				minDist = tmpDist
				minIdx = i
			}
		}
		return rs.kBuckets[kBuckNum][minIdx]
	}
	return nil
}

func (rs *routingState) getBucketNum(key dhtKey, self dhtKey) int {
	if self.Equals(key) {
		return -1
	}
	dist := self.xor(key)
	var distStr string
	for i, _ := range dist {
		distStr += fmt.Sprintf("%08b", dist[i])
	}
	var kBuckNum int
	for i, val := range distStr {
		if val == '1' {
			kBuckNum = keySpaceDim - i - 1
			break
		}
	}
	return kBuckNum
}

func (rs *routingState) addContact(cnt *contact, self dhtKey) (ret *contact) {
	if cnt == nil {
		return
	}
	if self.Equals(cnt.id) {
		return cnt
	}
	kBucketNum := rs.getBucketNum(cnt.id, self)

	if len(rs.kBuckets[kBucketNum]) >= rs.k {
		//TODO: ping contacts
		return cnt
	}
	for i, _ := range rs.kBuckets[kBucketNum] {
		if rs.kBuckets[kBucketNum][i].id == cnt.id {
			// TODO: refresh data & move to tail
			rs.kBuckets[kBucketNum][i].Update(cnt)
			return rs.kBuckets[kBucketNum][i]
		}
	}
	rs.kBuckets[kBucketNum] = append(rs.kBuckets[kBucketNum], cnt)
	return cnt
}

func (rs *routingState) rmContact(key dhtKey) {
	for i, _ := range rs.kBuckets {
		for j, val := range rs.kBuckets[i] {
			if val.id == key {
				rs.kBuckets[i] = append(rs.kBuckets[i][:j], rs.kBuckets[i][j+1:]...)
			}
		}
	}
}

func (rs *routingState) getStoredValue(key dhtKey) *nodeOrValue {
	val, ok := rs.store[key]
	if !ok {
		return nil
	}
	return val
}

func (rs *routingState) storeValue(key dhtKey, value *nodeOrValue) {
	rs.store[key] = value
}

func newRoutingState(k int) (ret *routingState) {
	ret = &routingState{
		neighborhoodSet: make([]*contact, 0),
		masterSet:       make([]*contact, 0),
		k:               k,
		master:          &contact{id: NewZeroDhtKey()},
		store:           make(map[dhtKey]*nodeOrValue),
	}
	for i, _ := range ret.kBuckets {
		ret.kBuckets[i] = make([]*contact, 0)
	}
	return
}

func newRoutingStateBs(masters []*contact, k int) (ret *routingState) {
	ret = newRoutingState(k)
	ret.masterSet = append(ret.masterSet, masters...)
	return
}
