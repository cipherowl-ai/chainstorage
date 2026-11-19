package ethereum

import (
	"encoding/json"
	"strings"

	geth "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/xerrors"
)

const (
	// borExtraSealLength is the length of the block producer's signature at the end of extraData.
	borExtraSealLength = crypto.SignatureLength

	ethBloomLength = 256
	ethNonceLength = 8
)

// borSealHeader holds the header fields that the Bor block producer signs.
type borSealHeader struct {
	ParentHash  geth.Hash      `json:"parentHash"`
	UncleHash   geth.Hash      `json:"sha3Uncles"`
	Coinbase    geth.Address   `json:"miner"`
	Root        geth.Hash      `json:"stateRoot"`
	TxHash      geth.Hash      `json:"transactionsRoot"`
	ReceiptHash geth.Hash      `json:"receiptsRoot"`
	Bloom       hexutil.Bytes  `json:"logsBloom"`
	Difficulty  *hexutil.Big   `json:"difficulty"`
	Number      *hexutil.Big   `json:"number"`
	GasLimit    hexutil.Uint64 `json:"gasLimit"`
	GasUsed     hexutil.Uint64 `json:"gasUsed"`
	Time        hexutil.Uint64 `json:"timestamp"`
	Extra       hexutil.Bytes  `json:"extraData"`
	MixDigest   geth.Hash      `json:"mixHash"`
	Nonce       hexutil.Bytes  `json:"nonce"`
	BaseFee     *hexutil.Big   `json:"baseFeePerGas"`
}

// recoverBorAuthor returns the producer (author) of a Polygon block as a JSON string,
// in the same format that bor_getAuthor returns.
//
// Bor has no coinbase in the header (miner is always the zero address). The producer signs the seal hash
// and puts the 65-byte signature at the end of extraData, so the producer is recovered from that signature.
// This is how Bor itself finds the producer, so it needs no bor_* RPC method and works with any provider
// that serves eth_getBlockByNumber.
func recoverBorAuthor(rawHeader json.RawMessage) (json.RawMessage, error) {
	var header borSealHeader
	if err := json.Unmarshal(rawHeader, &header); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal polygon block header: %w", err)
	}

	if header.Number == nil || header.Difficulty == nil {
		return nil, xerrors.New("polygon block header is missing number or difficulty")
	}

	var author string
	if header.Number.ToInt().Sign() == 0 {
		// The genesis block is not signed.
		author = ethNullAddress
	} else {
		address, err := header.recoverSigner()
		if err != nil {
			return nil, xerrors.Errorf("failed to recover author of polygon block %v: %w", header.Number.ToInt(), err)
		}

		author = strings.ToLower(address.Hex())
	}

	result, err := json.Marshal(author)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal polygon block author: %w", err)
	}

	return result, nil
}

func (h *borSealHeader) recoverSigner() (geth.Address, error) {
	if len(h.Extra) < borExtraSealLength {
		return geth.Address{}, xerrors.Errorf("extraData is %d bytes, shorter than the %d-byte signature", len(h.Extra), borExtraSealLength)
	}
	if len(h.Bloom) != ethBloomLength {
		return geth.Address{}, xerrors.Errorf("logsBloom is %d bytes, expected %d", len(h.Bloom), ethBloomLength)
	}
	if len(h.Nonce) != ethNonceLength {
		return geth.Address{}, xerrors.Errorf("nonce is %d bytes, expected %d", len(h.Nonce), ethNonceLength)
	}

	sealHash, err := h.sealHash()
	if err != nil {
		return geth.Address{}, err
	}

	signature := h.Extra[len(h.Extra)-borExtraSealLength:]
	publicKey, err := crypto.SigToPub(sealHash, signature)
	if err != nil {
		return geth.Address{}, xerrors.Errorf("failed to recover public key from signature: %w", err)
	}

	return crypto.PubkeyToAddress(*publicKey), nil
}

// sealHash mirrors Bor's SealHash: the keccak256 of the RLP-encoded header with the signature removed from extraData.
// Bor appends baseFee from the Jaipur fork on. Jaipur and London activate at the same height on Polygon mainnet and
// Amoy, so a header has baseFeePerGas exactly when it must be signed.
func (h *borSealHeader) sealHash() ([]byte, error) {
	fields := []any{
		h.ParentHash,
		h.UncleHash,
		h.Coinbase,
		h.Root,
		h.TxHash,
		h.ReceiptHash,
		[]byte(h.Bloom),
		h.Difficulty.ToInt(),
		h.Number.ToInt(),
		uint64(h.GasLimit),
		uint64(h.GasUsed),
		uint64(h.Time),
		[]byte(h.Extra[:len(h.Extra)-borExtraSealLength]),
		h.MixDigest,
		[]byte(h.Nonce),
	}
	if h.BaseFee != nil {
		fields = append(fields, h.BaseFee.ToInt())
	}

	encoded, err := rlp.EncodeToBytes(fields)
	if err != nil {
		return nil, xerrors.Errorf("failed to rlp-encode seal header: %w", err)
	}

	return crypto.Keccak256(encoded), nil
}
