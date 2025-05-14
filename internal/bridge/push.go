package bridge

import (
	"encoding/base64"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/valyala/fasthttp"
	"tonconnect-bridge/internal/bridge/metrics"
)

var (
	dataChunkSizeClasses = []int{
		1 << 2,
		1 << 4,
		1 << 6,
		1 << 8,
		1 << 10,
		2 << 10,
		4 << 10,
		8 << 10,
		16 << 10,
		32 << 10,
		64 << 10,
	}

	decodedBufPools = [...]sync.Pool{
		{New: func() any { return make([]byte, 1<<2) }},
		{New: func() any { return make([]byte, 1<<4) }},
		{New: func() any { return make([]byte, 1<<6) }},
		{New: func() any { return make([]byte, 1<<8) }},
		{New: func() any { return make([]byte, 1<<10) }},
		{New: func() any { return make([]byte, 2<<10) }},
		{New: func() any { return make([]byte, 4<<10) }},
		{New: func() any { return make([]byte, 8<<10) }},
		{New: func() any { return make([]byte, 16<<10) }},
		{New: func() any { return make([]byte, 32<<10) }},
		{New: func() any { return make([]byte, 64<<10) }},
	}
)

func getDecodedBufChunk(size int) []byte {
	i := 0
	for ; i < len(dataChunkSizeClasses)-1; i++ {
		if size <= dataChunkSizeClasses[i] {
			break
		}
	}
	return decodedBufPools[i].Get().([]byte)
}

func putDecodedBufChunk(p []byte) {
	for i, n := range dataChunkSizeClasses {
		if len(p) == n {
			decodedBufPools[i].Put(p)
			return
		}
	}
}

func (s *SSE) handlePush(ctx *fasthttp.RequestCtx, ip string, authorized bool) {
	if !ctx.IsPost() {
		respError(ctx, "request method is not supported", 400)
		return
	}

	if !authorized && s.pushLimiter.Add(ip, 1) < 1 {
		respError(ctx, "too many push operations per second", 429)
		return
	}

	clientId := string(ctx.QueryArgs().Peek("client_id"))
	if clientId == "" || len(clientId) > 64 {
		respError(ctx, "invalid client_id", 400)
		return
	}

	to := string(ctx.QueryArgs().Peek("to"))
	if to == "" || len(to) > 64 {
		respError(ctx, "invalid to", 400)
		return
	}

	ttl, err := ctx.QueryArgs().GetUint("ttl")
	if err != nil {
		respError(ctx, "ttl is invalid", 400)
		return
	}

	if ttl > s.MaxTTL {
		respError(ctx, "ttl is too long", 400)
		return
	}

	body := ctx.PostBody()
	decodedLen := base64.StdEncoding.DecodedLen(len(body))
	if decodedLen > 64*1024 {
		respError(ctx, "too big message", 400)
		return
	}

	decodedBuf := getDecodedBufChunk(decodedLen)
	defer putDecodedBufChunk(decodedBuf)

	decoded, err := base64.StdEncoding.Decode(decodedBuf[:decodedLen], body)
	if err != nil {
		respError(ctx, "invalid payload", 400)
		return
	}

	decodedData := decodedBuf[:decoded]

	cli := s.client(to, false)

	tm := time.Now()
	added := cli.Push(&Event{
		ID:       uint64(tm.UnixNano()),
		From:     clientId,
		Message:  decodedData,
		Deadline: tm.Unix() + int64(ttl),
	})
	if !added {
		log.Debug().Str("id", to).Msg("client buffer overflow")
		respError(ctx, "client's buffer overflow", 403)
		return
	}

	select {
	case cli.Signal <- struct{}{}:
	default:
	}

	if topic := string(ctx.QueryArgs().Peek("topic")); topic != "" {
		wh := WebhookData{
			ClientID: clientId,
			Topic:    topic,
			Hash:     decodedData,
		}

		for i, webhook := range s.webhooks {
			select {
			case webhook <- wh:
				log.Debug().Int("index", i).Str("topic", topic).Msg("webhook added to queue")
			default:
				// skip when overflow
				log.Warn().Int("index", i).Str("topic", topic).Msg("webhook buffer overflow")
			}
		}
	}

	metrics.Global.PushedMessages.Inc()
	respOk(ctx)
	return
}
