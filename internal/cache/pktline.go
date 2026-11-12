package cache

import (
	"strconv"
	"strings"
)

// request summarises the parts of an upload-pack POST body the cache cares about.
type request struct {
	command  string   // protocol v2 command ("ls-refs", "fetch", ...); empty for v1
	wants    []string // object IDs the client wants
	wantRefs bool     // v2 want-ref: names resolved server side, so we must sync first
}

// parseRequest scans pkt-lines in body. It is lenient: malformed input just
// ends the scan, and upload-pack itself is the authority on validity.
func parseRequest(b []byte) request {
	var r request
	for len(b) >= 4 {
		n, err := strconv.ParseUint(string(b[:4]), 16, 32)
		if err != nil {
			break
		}
		if n < 4 { // 0000 flush, 0001 delim, 0002 response-end
			b = b[4:]
			continue
		}
		if int(n) > len(b) {
			break
		}
		line := strings.TrimSuffix(string(b[4:n]), "\n")
		b = b[n:]
		switch {
		case strings.HasPrefix(line, "command="):
			r.command = strings.TrimPrefix(line, "command=")
		case strings.HasPrefix(line, "want "):
			if f := strings.Fields(line[len("want "):]); len(f) > 0 {
				r.wants = append(r.wants, f[0])
			}
		case strings.HasPrefix(line, "want-ref "):
			r.wantRefs = true
		}
	}
	return r
}

func pktLine(s string) string {
	return strconv.FormatInt(int64(len(s)+4)+0x10000, 16)[1:] + s
}
