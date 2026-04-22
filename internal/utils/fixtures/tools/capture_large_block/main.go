// Capture large-block RPC fixtures for memory benchmarks.
//
// This is a one-time developer utility. Given an RPC endpoint URL and a
// JSON-RPC method/params, it issues a single request and writes the raw
// response body (the full JSON-RPC envelope, exactly as the wire returned
// it) to the specified output file. Those files are then committed to
// internal/utils/fixtures/client/<chain>/large/ and replayed by the memory
// benchmark via a mocked HTTP transport.
//
// Example (Ethereum debug_traceBlockByHash on a large block):
//
//	go run ./internal/utils/fixtures/tools/capture_large_block \
//	    --url "$QUICKNODE_ETHEREUM_URL" \
//	    --method debug_traceBlockByHash \
//	    --params '["0xBLOCKHASH",{"tracer":"callTracer","timeout":"90s"}]' \
//	    --output internal/utils/fixtures/client/ethereum/large/eth_traceblockbyhash.json
//
// Batch mode (e.g., Ethereum trace batch across many blocks, or Bitcoin
// getrawtransaction for many input tx IDs):
//
//	go run ./internal/utils/fixtures/tools/capture_large_block \
//	    --url "$NOWNODES_BITCOIN_URL" \
//	    --method getrawtransaction \
//	    --batch-params-file inputs.json \
//	    --output internal/utils/fixtures/client/bitcoin/large/btc_inputs_batch.json
//
// Credentials:
//   - --user/--password flags, OR
//   - RPC_BASIC_USER / RPC_BASIC_PASSWORD env vars, OR
//   - URL with embedded credentials (https://user:pass@host/...)
//
// Do NOT commit secrets; the captured response bodies are what goes in the repo.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type request struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
	ID      uint   `json:"id"`
}

func main() {
	var (
		url             = flag.String("url", "", "RPC endpoint URL (required)")
		method          = flag.String("method", "", "JSON-RPC method name, e.g. debug_traceBlockByHash (required)")
		paramsJSON      = flag.String("params", "", "JSON-encoded params array for a single call, e.g. '[\"0xhash\",{\"tracer\":\"callTracer\"}]'")
		batchParamsFile = flag.String("batch-params-file", "", "Path to a file containing a JSON array of params arrays, one per batch element")
		output          = flag.String("output", "", "Output file path (required)")
		user            = flag.String("user", os.Getenv("RPC_BASIC_USER"), "Basic auth user (defaults to $RPC_BASIC_USER)")
		password        = flag.String("password", os.Getenv("RPC_BASIC_PASSWORD"), "Basic auth password (defaults to $RPC_BASIC_PASSWORD)")
		timeoutSec      = flag.Int("timeout", 180, "HTTP timeout in seconds")
	)
	flag.Parse()

	if *url == "" || *method == "" || *output == "" {
		fmt.Fprintln(os.Stderr, "Usage: capture_large_block --url URL --method METHOD --output PATH [--params JSON | --batch-params-file PATH]")
		flag.PrintDefaults()
		os.Exit(2)
	}
	if (*paramsJSON == "") == (*batchParamsFile == "") {
		fmt.Fprintln(os.Stderr, "exactly one of --params or --batch-params-file must be set")
		os.Exit(2)
	}

	body, err := buildRequestBody(*method, *paramsJSON, *batchParamsFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build request: %v\n", err)
		os.Exit(1)
	}

	n, err := postToFile(*url, *user, *password, body, *output, time.Duration(*timeoutSec)*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "request failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "wrote %d bytes to %s\n", n, *output)
}

func buildRequestBody(method, paramsJSON, batchParamsFile string) ([]byte, error) {
	if paramsJSON != "" {
		var params any
		if err := json.Unmarshal([]byte(paramsJSON), &params); err != nil {
			return nil, fmt.Errorf("invalid --params JSON: %w", err)
		}
		return json.Marshal(request{JSONRPC: "2.0", Method: method, Params: params, ID: 0})
	}

	raw, err := os.ReadFile(batchParamsFile)
	if err != nil {
		return nil, fmt.Errorf("read --batch-params-file: %w", err)
	}
	var batchParams []any
	if err := json.Unmarshal(raw, &batchParams); err != nil {
		return nil, fmt.Errorf("batch params file must contain a JSON array of params arrays: %w", err)
	}
	batch := make([]request, len(batchParams))
	for i, p := range batchParams {
		batch[i] = request{JSONRPC: "2.0", Method: method, Params: p, ID: uint(i)}
	}
	return json.Marshal(batch)
}

// postToFile issues the HTTP request and streams the response body directly
// to the output file via io.Copy, avoiding io.ReadAll which would buffer the
// entire response in memory — a hazard for this tool since its purpose is
// capturing large-block payloads that may be hundreds of MB.
func postToFile(url, user, password string, body []byte, outputPath string, timeout time.Duration) (int64, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if user != "" && password != "" {
		req.SetBasicAuth(user, password)
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("http do: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		// Error body is expected to be small; buffering up to 1 KB for the
		// diagnostic message is fine.
		preview, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return 0, fmt.Errorf("http %d: %s", resp.StatusCode, truncate(string(preview), 500))
	}

	out, err := os.Create(outputPath)
	if err != nil {
		return 0, fmt.Errorf("create output: %w", err)
	}
	defer func() { _ = out.Close() }()

	n, err := io.Copy(out, resp.Body)
	if err != nil {
		return n, fmt.Errorf("stream response to file: %w", err)
	}
	return n, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
