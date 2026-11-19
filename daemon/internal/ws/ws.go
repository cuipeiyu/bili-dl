// Package ws 实现最小 WebSocket 服务端，仅依赖标准库。
// 只处理文本帧、ping/pong、close，够本项目进度推送使用，避免引入外部依赖。
package ws

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"net"
	"net/http"
	"sync"
)

const magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

var (
	ErrClosed = errors.New("ws closed")
	errTooBig = errors.New("ws frame too large")
)

// Conn 是一条 WS 连接。写操作必须持 writeMu。
type Conn struct {
	net.Conn
	br      *bufio.Reader
	writeMu sync.Mutex
}

// Upgrade 完成 HTTP→WS 握手。
func Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("hijack not supported")
	}
	conn, br, err := hj.Hijack()
	if err != nil {
		return nil, err
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		conn.Close()
		return nil, errors.New("missing Sec-WebSocket-Key")
	}
	sum := sha1.Sum([]byte(key + magic))
	accept := base64.StdEncoding.EncodeToString(sum[:])
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := br.WriteString(resp); err != nil {
		conn.Close()
		return nil, err
	}
	if err := br.Flush(); err != nil {
		conn.Close()
		return nil, err
	}
	return &Conn{Conn: conn, br: br.Reader}, nil
}

// ReadMessage 读取一条完整文本消息，内部自动应答 ping。
func (c *Conn) ReadMessage(max int64) ([]byte, error) {
	var payload []byte
	for {
		frame, opcode, fin, err := c.readFrame(max)
		if err != nil {
			return nil, err
		}
		switch opcode {
		case 0x1: // text
			payload = append(payload, frame...)
		case 0x2: // binary，按文本处理
			payload = append(payload, frame...)
		case 0x0: // continuation
			payload = append(payload, frame...)
		case 0x9: // ping
			if err := c.WriteFrame(0xA, frame); err != nil {
				return nil, err
			}
			continue
		case 0xA: // pong
			continue
		case 0x8: // close
			return nil, ErrClosed
		default:
			continue
		}
		if int64(len(payload)) > max {
			return nil, errTooBig
		}
		if fin {
			return payload, nil
		}
	}
}

func (c *Conn) readFrame(max int64) ([]byte, byte, bool, error) {
	b1, err := c.br.ReadByte()
	if err != nil {
		return nil, 0, false, err
	}
	fin := b1&0x80 != 0
	opcode := b1 & 0x0F
	b2, err := c.br.ReadByte()
	if err != nil {
		return nil, 0, false, err
	}
	masked := b2&0x80 != 0
	length := int64(b2 & 0x7F)
	switch length {
	case 126:
		var b [2]byte
		if _, err := readFull(c.br, b[:]); err != nil {
			return nil, 0, false, err
		}
		length = int64(binary.BigEndian.Uint16(b[:]))
	case 127:
		var b [8]byte
		if _, err := readFull(c.br, b[:]); err != nil {
			return nil, 0, false, err
		}
		length = int64(binary.BigEndian.Uint64(b[:]))
	}
	if length > max {
		return nil, 0, false, errTooBig
	}
	var mask [4]byte
	if masked {
		if _, err := readFull(c.br, mask[:]); err != nil {
			return nil, 0, false, err
		}
	}
	buf := make([]byte, length)
	if _, err := readFull(c.br, buf); err != nil {
		return nil, 0, false, err
	}
	if masked {
		for i := range buf {
			buf[i] ^= mask[i%4]
		}
	}
	return buf, opcode, fin, nil
}

func readFull(br *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := br.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// WriteFrame 发送一帧（服务端帧不掩码）。
func (c *Conn) WriteFrame(opcode byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	var header []byte
	switch {
	case len(payload) < 126:
		header = []byte{0x80 | opcode, byte(len(payload))}
	case len(payload) < 65536:
		header = make([]byte, 4)
		header[0], header[1] = 0x80|opcode, 126
		binary.BigEndian.PutUint16(header[2:], uint16(len(payload)))
	default:
		header = make([]byte, 10)
		header[0], header[1] = 0x80|opcode, 127
		binary.BigEndian.PutUint64(header[2:], uint64(len(payload)))
	}
	if _, err := c.Conn.Write(header); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := c.Conn.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// Hub 管理所有连接并广播。
type Hub struct {
	mu      sync.RWMutex
	clients map[*Conn]struct{}
}

func NewHub() *Hub {
	return &Hub{clients: make(map[*Conn]struct{})}
}

func (h *Hub) Add(c *Conn) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *Hub) Remove(c *Conn) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

// Len 返回当前连接数。
func (h *Hub) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// Broadcast 向所有连接广播；写失败的连接直接移除。
func (h *Hub) Broadcast(opcode byte, payload []byte) {
	h.mu.RLock()
	clients := make([]*Conn, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()

	for _, c := range clients {
		if err := c.WriteFrame(opcode, payload); err != nil {
			h.Remove(c)
			c.Close()
		}
	}
}

