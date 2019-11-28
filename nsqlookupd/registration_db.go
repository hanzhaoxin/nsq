package nsqlookupd

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

/**
结构示意：
RegistrationDB：
	Key：Registration
		Category
		Key
		SubKey
	Value:ProducerMap
		Key:Producer.id
		Value:Producer
 */


// 注册库
type RegistrationDB struct {
	sync.RWMutex

	/**
	 *key：注册项
	 *value：生产者集合
	 */
	registrationMap map[Registration]ProducerMap
}

// 注册项
type Registration struct {
	Category string //["topic"/"channel"/"client"]
	Key      string //[topic]
	SubKey   string //[""/channel]
}
// 注册项集合
type Registrations []Registration

// 成员信息
type PeerInfo struct {
	lastUpdate       int64	// 最后一次更新时间
	id               string	// 唯一标识
	RemoteAddress    string `json:"remote_address"`	// 远程地址
	Hostname         string `json:"hostname"`	// 域名
	BroadcastAddress string `json:"broadcast_address"`	// 广播地址
	TCPPort          int    `json:"tcp_port"`	//	tcp端口
	HTTPPort         int    `json:"http_port"`	//	http端口
	Version          string `json:"version"`	//	版本
}

// 生产者（nsqd）
type Producer struct {
	peerInfo     *PeerInfo	//成员信息
	tombstoned   bool	// 逻辑删除
	tombstonedAt time.Time	// 逻辑删除时间
}

// 生产者（nsqd）集合
type Producers []*Producer
type ProducerMap map[string]*Producer

// 生产者.tostring（）
func (p *Producer) String() string {
	return fmt.Sprintf("%s [%d, %d]", p.peerInfo.BroadcastAddress, p.peerInfo.TCPPort, p.peerInfo.HTTPPort)
}

// 生产者.逻辑删除()
func (p *Producer) Tombstone() {
	p.tombstoned = true
	p.tombstonedAt = time.Now()
}

// 生产者.是否已逻辑删除(生产者保持 逻辑删除 的时长)
func (p *Producer) IsTombstoned(lifetime time.Duration) bool {
	return p.tombstoned && time.Now().Sub(p.tombstonedAt) < lifetime
}

// 注册库构造器
func NewRegistrationDB() *RegistrationDB {
	return &RegistrationDB{
		registrationMap: make(map[Registration]ProducerMap),
	}
}

// 注册库.添加一个注册项
func (r *RegistrationDB) AddRegistration(k Registration) {
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k]
	if !ok {
		r.registrationMap[k] = make(map[string]*Producer)
	}
}

// 注册库.添加一个生产者
func (r *RegistrationDB) AddProducer(k Registration, p *Producer) bool {
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k]
	if !ok {
		r.registrationMap[k] = make(map[string]*Producer)
	}
	producers := r.registrationMap[k]
	_, found := producers[p.peerInfo.id]
	if found == false {
		producers[p.peerInfo.id] = p
	}
	return !found
}

// 注册库.移除一个生产者
func (r *RegistrationDB) RemoveProducer(k Registration, id string) (bool, int) {
	r.Lock()
	defer r.Unlock()
	producers, ok := r.registrationMap[k]
	if !ok {
		return false, 0
	}
	removed := false
	if _, exists := producers[id]; exists {
		removed = true
	}

	// Note: this leaves keys in the DB even if they have empty lists
	delete(producers, id)
	return removed, len(producers)
}

// 注册库.移除注册项（及该key下的所有生产者）
func (r *RegistrationDB) RemoveRegistration(k Registration) {
	r.Lock()
	defer r.Unlock()
	delete(r.registrationMap, k)
}

// 是否需要过滤
func (r *RegistrationDB) needFilter(key string, subkey string) bool {
	return key == "*" || subkey == "*"
}

// 查找符合条件的注册项
func (r *RegistrationDB) FindRegistrations(category string, key string, subkey string) Registrations {
	r.RLock()
	defer r.RUnlock()
	if !r.needFilter(key, subkey) {
		k := Registration{category, key, subkey}
		if _, ok := r.registrationMap[k]; ok {
			return Registrations{k}
		}
		return Registrations{}
	}
	results := Registrations{}
	for k := range r.registrationMap {
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		results = append(results, k)
	}
	return results
}

// 查找符合条件的生产者
func (r *RegistrationDB) FindProducers(category string, key string, subkey string) Producers {
	r.RLock()
	defer r.RUnlock()
	if !r.needFilter(key, subkey) {
		k := Registration{category, key, subkey}
		return ProducerMap2Slice(r.registrationMap[k])
	}

	results := make(map[string]struct{})
	var retProducers Producers
	for k, producers := range r.registrationMap {
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		for _, producer := range producers {
			_, found := results[producer.peerInfo.id]
			if found == false {
				results[producer.peerInfo.id] = struct{}{}
				retProducers = append(retProducers, producer)
			}
		}
	}
	return retProducers
}

// 查找有某个生产者的所有注册项
func (r *RegistrationDB) LookupRegistrations(id string) Registrations {
	r.RLock()
	defer r.RUnlock()
	results := Registrations{}
	for k, producers := range r.registrationMap {
		if _, exists := producers[id]; exists {
			results = append(results, k)
		}
	}
	return results
}

// 是否匹配
func (k Registration) IsMatch(category string, key string, subkey string) bool {
	if category != k.Category {
		return false
	}
	if key != "*" && k.Key != key {
		return false
	}
	if subkey != "*" && k.SubKey != subkey {
		return false
	}
	return true
}

// 过滤出符合条件的注册项
func (rr Registrations) Filter(category string, key string, subkey string) Registrations {
	output := Registrations{}
	for _, k := range rr {
		if k.IsMatch(category, key, subkey) {
			output = append(output, k)
		}
	}
	return output
}

// 注册项集合-> 注册项Key集合
func (rr Registrations) Keys() []string {
	keys := make([]string, len(rr))
	for i, k := range rr {
		keys[i] = k.Key
	}
	return keys
}

// 注册项集合-> 注册项SubKey集合
func (rr Registrations) SubKeys() []string {
	subkeys := make([]string, len(rr))
	for i, k := range rr {
		subkeys[i] = k.SubKey
	}
	return subkeys
}

// 过滤活着的生产者
func (pp Producers) FilterByActive(inactivityTimeout time.Duration, tombstoneLifetime time.Duration) Producers {
	now := time.Now()
	results := Producers{}
	for _, p := range pp {
		cur := time.Unix(0, atomic.LoadInt64(&p.peerInfo.lastUpdate))
		if now.Sub(cur) > inactivityTimeout || p.IsTombstoned(tombstoneLifetime) {
			continue
		}
		results = append(results, p)
	}
	return results
}

// 生产者集合->成员信息集合
func (pp Producers) PeerInfo() []*PeerInfo {
	results := []*PeerInfo{}
	for _, p := range pp {
		results = append(results, p.peerInfo)
	}
	return results
}

// values
func ProducerMap2Slice(pm ProducerMap) Producers {
	var producers Producers
	for _, producer := range pm {
		producers = append(producers, producer)
	}

	return producers
}
