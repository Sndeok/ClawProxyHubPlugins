package qodersign

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// ServerPubKeyPEM 上游 COSY 服务端公钥（客户端硬编码同一份）。
const ServerPubKeyPEM = `-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDA8iMH5c02LilrsERw9t6Pv5Nc
4k6Pz1EaDicBMpdpxKduSZu5OANqUq8er4GM95omAGIOPOh+Nx0spthYA2BqGz+l
6HRkPJ7S236FZz73In/KVuLnwI8JJ2CbuJap8kvheCCZpmAWpb/cPx/3Vr/J6I17
XcW+ML9FoCI6AOvOzwIDAQAB
-----END PUBLIC KEY-----`

var cosyPubKey *rsa.PublicKey

func init() {
	block, _ := pem.Decode([]byte(ServerPubKeyPEM))
	if block == nil {
		panic("qodersign: bad server pubkey PEM")
	}
	k, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		panic(err)
	}
	pk, ok := k.(*rsa.PublicKey)
	if !ok {
		panic("qodersign: server pubkey is not RSA")
	}
	cosyPubKey = pk
}

// Identity 参与 COSY 身份加密的账号身份字段（键名与上游一致，勿改）。
type Identity struct {
	Name               string
	Aid                string
	Uid                string
	YxUid              string
	OrganizationID     string
	OrganizationName   string
	UserType           string
	SecurityOauthToken string
	RefreshToken       string
}

// Session 一次签名的会话上下文（每个账号一份；dt/drt 变化后需重建）。
type Session struct {
	MachineID    string
	MachineToken string
	MachineType  string
	TempKey      []byte // 16 字节 ASCII（与桌面端 uuid.hex[:16] 对齐）
	CosyKey      string // base64(RSA_PKCS1v15(tempKey))
	Info         string // base64(AES-128-CBC(identity JSON))
}

// NewSession 构建会话：随机 tempKey → RSA 包裹；identity → AES-CBC 加密。
func NewSession(id Identity, machineID, machineToken, machineType string) (*Session, error) {
	if machineID == "" || machineToken == "" || machineType == "" {
		return nil, fmt.Errorf("qodersign: machine fingerprint incomplete")
	}
	tempKey := []byte(randomHex(16))
	wrapped, err := rsa.EncryptPKCS1v15(rand.Reader, cosyPubKey, tempKey)
	if err != nil {
		return nil, fmt.Errorf("qodersign: rsa wrap temp key: %w", err)
	}
	infoCipher, err := aesCBCEncrypt(identityJSON(id), tempKey)
	if err != nil {
		return nil, fmt.Errorf("qodersign: encrypt identity: %w", err)
	}
	return &Session{
		MachineID:    machineID,
		MachineToken: machineToken,
		MachineType:  machineType,
		TempKey:      tempKey,
		CosyKey:      base64.StdEncoding.EncodeToString(wrapped),
		Info:         base64.StdEncoding.EncodeToString(infoCipher),
	}, nil
}

// HeaderConfig 不同产品的 COSY 版本号与附加头。
type HeaderConfig struct {
	CosyVersion  string            // 上游 cosy-version（Qoder 1.0.10 / QoderWork 0.1.43）
	Scene        string            // cosy-scene（客户端 ClientMetadata.scene）
	Product      string            // cosy-business-product（客户端 ClientMetadata.business_product）
	BusinessType string            // cosy-business-type（客户端 ClientMetadata.business_type）
	ClientType   string            // cosy-clienttype（客户端 ClientMetadata.client_type；空=5）
	ClientIP     string            // cosy-clientip，空则不发送
	DataPolicy   string            // cosy-data-policy（Qoder: agree / QoderWork: AGREE）
	MachineOS    string            // cosy-machineos（千问办公要 x86_64_win32；空则不发送）
	UserAgent    string            // 覆盖出站 UA（千问办公官方是 qwenwork/<ver>；空=Go-http-client/2.0）
	OmitMachineType bool           // true 时不发 cosy-machinetype（qwenwork2api 的可用配方不带它）
	OmitClientIP    bool           // true 时不发 cosy-clientip
	Extra        map[string]string // 其它固定头
}

// PathForSignature 取路径签名部分（去掉 /algo 前缀）。
func PathForSignature(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("qodersign: parse url: %w", err)
	}
	return strings.TrimPrefix(u.Path, "/algo"), nil
}

// ApplyHeaders 设置全部 COSY 头（含 Authorization）。
func (s *Session) ApplyHeaders(req *http.Request, cfg HeaderConfig, body, uid, modelKey string) error {
	payload := map[string]string{
		"cosyVersion": orDefault(cfg.CosyVersion, "1.0.10"),
		"ideVersion":  "",
		"info":        s.Info,
		"requestId":   UUID4(),
		"version":     "v1",
	}
	payloadB64 := base64.StdEncoding.EncodeToString(sortedCompact(payload))
	pathSig, err := PathForSignature(req.URL.String())
	if err != nil {
		return err
	}
	date := fmt.Sprintf("%d", time.Now().Unix())
	sig := md5Hex(payloadB64 + "\n" + s.CosyKey + "\n" + date + "\n" + body + "\n" + pathSig)

	h := req.Header
	h.Set("authorization", "Bearer COSY."+payloadB64+"."+sig)
	h.Set("content-type", "application/json")
	h.Set("cosy-key", s.CosyKey)
	h.Set("cosy-user", uid)
	h.Set("cosy-date", date)
	if !cfg.OmitMachineType {
		h.Set("cosy-machinetype", s.MachineType)
	}
	h.Set("cosy-machineid", s.MachineID)
	h.Set("cosy-machinetoken", s.MachineToken)
	if cfg.MachineOS != "" {
		h.Set("cosy-machineos", cfg.MachineOS)
	}
	h.Set("cosy-clienttype", orDefault(cfg.ClientType, "5"))
	h.Set("cosy-version", orDefault(cfg.CosyVersion, "1.0.10"))
	h.Set("login-version", "v2")
	h.Set("accept", "text/event-stream")
	h.Set("accept-encoding", "identity")
	h.Set("cache-control", "no-cache")
	h.Set("user-agent", orDefault(cfg.UserAgent, "Go-http-client/2.0"))
	if cfg.DataPolicy != "" {
		h.Set("cosy-data-policy", cfg.DataPolicy)
	}
	if cfg.Scene != "" {
		h.Set("cosy-scene", cfg.Scene)
	}
	if cfg.Product != "" {
		h.Set("cosy-business-product", cfg.Product)
	}
	if cfg.BusinessType != "" {
		h.Set("cosy-business-type", cfg.BusinessType)
	}
	// 参考实现（qoderwork2api）固定下发 cosy-clientip，缺失时上游可能判为非法客户端
	if !cfg.OmitClientIP {
		h.Set("cosy-clientip", orDefault(cfg.ClientIP, "169.254.198.161"))
	}
	if modelKey != "" { // Qoder 系用 x-model-key 指定上游模型
		h.Set("x-model-key", modelKey)
		h.Set("x-model-source", "system")
	}
	for k, v := range cfg.Extra {
		h.Set(k, v)
	}
	return nil
}

// identityJSON 身份字段序列化：键排序 + 无空白（参与加密，必须稳定）。
func identityJSON(id Identity) []byte {
	return sortedCompact(map[string]string{
		"name":                 id.Name,
		"aid":                  id.Aid,
		"uid":                  id.Uid,
		"yx_uid":               id.YxUid,
		"organization_id":      id.OrganizationID,
		"organization_name":    id.OrganizationName,
		"user_type":            id.UserType,
		"security_oauth_token": id.SecurityOauthToken,
		"refresh_token":        id.RefreshToken,
	})
}

// sortedCompact 按 key 排序、无空白序列化（手写以保证与客户端字节一致）。
func sortedCompact(m map[string]string) []byte {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	sb.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		kb, _ := json.Marshal(k)
		vb, _ := json.Marshal(m[k])
		sb.Write(kb)
		sb.WriteByte(':')
		sb.Write(vb)
	}
	sb.WriteByte('}')
	return []byte(sb.String())
}

// aesCBCEncrypt AES-128-CBC，key = iv，PKCS7 padding。
func aesCBCEncrypt(plain, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	bs := block.BlockSize()
	padLen := bs - len(plain)%bs
	padded := make([]byte, len(plain)+padLen)
	copy(padded, plain)
	for i := len(plain); i < len(padded); i++ {
		padded[i] = byte(padLen)
	}
	out := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, key[:bs]).CryptBlocks(out, padded)
	return out, nil
}

// MD5Hex 上游签名摘要。
func MD5Hex(s string) string { return md5Hex(s) }

func md5Hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

// UUID4 随机 UUID v4 字符串。
func UUID4() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)[:n]
}

func orDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}
