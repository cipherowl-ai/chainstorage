// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.32.0
// 	protoc        v4.25.2
// source: coinbase/c3/common/common.proto

package common

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// Blockchain defines an enumeration of supported blockchains.
// The field numbers are not continuous because only a subset is open sourced.
type Blockchain int32

const (
	Blockchain_BLOCKCHAIN_UNKNOWN     Blockchain = 0
	Blockchain_BLOCKCHAIN_SOLANA      Blockchain = 11
	Blockchain_BLOCKCHAIN_BITCOIN     Blockchain = 16
	Blockchain_BLOCKCHAIN_ETHEREUM    Blockchain = 17
	Blockchain_BLOCKCHAIN_BITCOINCASH Blockchain = 18
	Blockchain_BLOCKCHAIN_LITECOIN    Blockchain = 19
	Blockchain_BLOCKCHAIN_DOGECOIN    Blockchain = 26
	Blockchain_BLOCKCHAIN_TRON        Blockchain = 30
	Blockchain_BLOCKCHAIN_BSC         Blockchain = 31
	Blockchain_BLOCKCHAIN_AVACCHAIN   Blockchain = 32
	Blockchain_BLOCKCHAIN_POLYGON     Blockchain = 35
	Blockchain_BLOCKCHAIN_OPTIMISM    Blockchain = 39
	Blockchain_BLOCKCHAIN_ARBITRUM    Blockchain = 41
	Blockchain_BLOCKCHAIN_APTOS       Blockchain = 47 // L1 network using the Move language (originally created for Libra/Diem)
	Blockchain_BLOCKCHAIN_FANTOM      Blockchain = 51
	Blockchain_BLOCKCHAIN_BASE        Blockchain = 56 // Coinbase L2
	Blockchain_BLOCKCHAIN_STORY       Blockchain = 60
)

// Enum value maps for Blockchain.
var (
	Blockchain_name = map[int32]string{
		0:  "BLOCKCHAIN_UNKNOWN",
		11: "BLOCKCHAIN_SOLANA",
		16: "BLOCKCHAIN_BITCOIN",
		17: "BLOCKCHAIN_ETHEREUM",
		18: "BLOCKCHAIN_BITCOINCASH",
		19: "BLOCKCHAIN_LITECOIN",
		26: "BLOCKCHAIN_DOGECOIN",
		30: "BLOCKCHAIN_TRON",
		31: "BLOCKCHAIN_BSC",
		32: "BLOCKCHAIN_AVACCHAIN",
		35: "BLOCKCHAIN_POLYGON",
		39: "BLOCKCHAIN_OPTIMISM",
		41: "BLOCKCHAIN_ARBITRUM",
		47: "BLOCKCHAIN_APTOS",
		51: "BLOCKCHAIN_FANTOM",
		56: "BLOCKCHAIN_BASE",
		60: "BLOCKCHAIN_STORY",
	}
	Blockchain_value = map[string]int32{
		"BLOCKCHAIN_UNKNOWN":     0,
		"BLOCKCHAIN_SOLANA":      11,
		"BLOCKCHAIN_BITCOIN":     16,
		"BLOCKCHAIN_ETHEREUM":    17,
		"BLOCKCHAIN_BITCOINCASH": 18,
		"BLOCKCHAIN_LITECOIN":    19,
		"BLOCKCHAIN_DOGECOIN":    26,
		"BLOCKCHAIN_TRON":        30,
		"BLOCKCHAIN_BSC":         31,
		"BLOCKCHAIN_AVACCHAIN":   32,
		"BLOCKCHAIN_POLYGON":     35,
		"BLOCKCHAIN_OPTIMISM":    39,
		"BLOCKCHAIN_ARBITRUM":    41,
		"BLOCKCHAIN_APTOS":       47,
		"BLOCKCHAIN_FANTOM":      51,
		"BLOCKCHAIN_BASE":        56,
		"BLOCKCHAIN_STORY":       60,
	}
)

func (x Blockchain) Enum() *Blockchain {
	p := new(Blockchain)
	*p = x
	return p
}

func (x Blockchain) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Blockchain) Descriptor() protoreflect.EnumDescriptor {
	return file_coinbase_c3_common_common_proto_enumTypes[0].Descriptor()
}

func (Blockchain) Type() protoreflect.EnumType {
	return &file_coinbase_c3_common_common_proto_enumTypes[0]
}

func (x Blockchain) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Blockchain.Descriptor instead.
func (Blockchain) EnumDescriptor() ([]byte, []int) {
	return file_coinbase_c3_common_common_proto_rawDescGZIP(), []int{0}
}

// Network defines an enumeration of supported networks.
// The field numbers are not continuous because only a subset is open sourced.
type Network int32

const (
	Network_NETWORK_UNKNOWN             Network = 0
	Network_NETWORK_SOLANA_MAINNET      Network = 22
	Network_NETWORK_SOLANA_TESTNET      Network = 23
	Network_NETWORK_BITCOIN_MAINNET     Network = 33
	Network_NETWORK_BITCOIN_TESTNET     Network = 34
	Network_NETWORK_ETHEREUM_MAINNET    Network = 35
	Network_NETWORK_ETHEREUM_TESTNET    Network = 36
	Network_NETWORK_BITCOINCASH_MAINNET Network = 37
	Network_NETWORK_BITCOINCASH_TESTNET Network = 38
	Network_NETWORK_LITECOIN_MAINNET    Network = 39
	Network_NETWORK_LITECOIN_TESTNET    Network = 40
	Network_NETWORK_TRON_MAINNET        Network = 64
	Network_NETWORK_TRON_TESTNET        Network = 65
	Network_NETWORK_ETHEREUM_GOERLI     Network = 66
	Network_NETWORK_DOGECOIN_MAINNET    Network = 56
	Network_NETWORK_DOGECOIN_TESTNET    Network = 57
	Network_NETWORK_BSC_MAINNET         Network = 70
	Network_NETWORK_BSC_TESTNET         Network = 71
	Network_NETWORK_AVACCHAIN_MAINNET   Network = 72
	Network_NETWORK_AVACCHAIN_TESTNET   Network = 73
	Network_NETWORK_POLYGON_MAINNET     Network = 78
	Network_NETWORK_POLYGON_TESTNET     Network = 79
	Network_NETWORK_OPTIMISM_MAINNET    Network = 86
	Network_NETWORK_OPTIMISM_TESTNET    Network = 87
	Network_NETWORK_ARBITRUM_MAINNET    Network = 91
	Network_NETWORK_ARBITRUM_TESTNET    Network = 92
	Network_NETWORK_APTOS_MAINNET       Network = 103
	Network_NETWORK_APTOS_TESTNET       Network = 104
	Network_NETWORK_FANTOM_MAINNET      Network = 111
	Network_NETWORK_FANTOM_TESTNET      Network = 112
	Network_NETWORK_BASE_MAINNET        Network = 123 // Coinbase L2 running on Ethereum mainnet
	Network_NETWORK_BASE_GOERLI         Network = 125 // Coinbase L2 running on Ethereum Goerli
	Network_NETWORK_ETHEREUM_HOLESKY    Network = 136
	Network_NETWORK_STORY_MAINNET       Network = 140
)

// Enum value maps for Network.
var (
	Network_name = map[int32]string{
		0:   "NETWORK_UNKNOWN",
		22:  "NETWORK_SOLANA_MAINNET",
		23:  "NETWORK_SOLANA_TESTNET",
		33:  "NETWORK_BITCOIN_MAINNET",
		34:  "NETWORK_BITCOIN_TESTNET",
		35:  "NETWORK_ETHEREUM_MAINNET",
		36:  "NETWORK_ETHEREUM_TESTNET",
		37:  "NETWORK_BITCOINCASH_MAINNET",
		38:  "NETWORK_BITCOINCASH_TESTNET",
		39:  "NETWORK_LITECOIN_MAINNET",
		40:  "NETWORK_LITECOIN_TESTNET",
		64:  "NETWORK_TRON_MAINNET",
		65:  "NETWORK_TRON_TESTNET",
		66:  "NETWORK_ETHEREUM_GOERLI",
		56:  "NETWORK_DOGECOIN_MAINNET",
		57:  "NETWORK_DOGECOIN_TESTNET",
		70:  "NETWORK_BSC_MAINNET",
		71:  "NETWORK_BSC_TESTNET",
		72:  "NETWORK_AVACCHAIN_MAINNET",
		73:  "NETWORK_AVACCHAIN_TESTNET",
		78:  "NETWORK_POLYGON_MAINNET",
		79:  "NETWORK_POLYGON_TESTNET",
		86:  "NETWORK_OPTIMISM_MAINNET",
		87:  "NETWORK_OPTIMISM_TESTNET",
		91:  "NETWORK_ARBITRUM_MAINNET",
		92:  "NETWORK_ARBITRUM_TESTNET",
		103: "NETWORK_APTOS_MAINNET",
		104: "NETWORK_APTOS_TESTNET",
		111: "NETWORK_FANTOM_MAINNET",
		112: "NETWORK_FANTOM_TESTNET",
		123: "NETWORK_BASE_MAINNET",
		125: "NETWORK_BASE_GOERLI",
		136: "NETWORK_ETHEREUM_HOLESKY",
		140: "NETWORK_STORY_MAINNET",
	}
	Network_value = map[string]int32{
		"NETWORK_UNKNOWN":             0,
		"NETWORK_SOLANA_MAINNET":      22,
		"NETWORK_SOLANA_TESTNET":      23,
		"NETWORK_BITCOIN_MAINNET":     33,
		"NETWORK_BITCOIN_TESTNET":     34,
		"NETWORK_ETHEREUM_MAINNET":    35,
		"NETWORK_ETHEREUM_TESTNET":    36,
		"NETWORK_BITCOINCASH_MAINNET": 37,
		"NETWORK_BITCOINCASH_TESTNET": 38,
		"NETWORK_LITECOIN_MAINNET":    39,
		"NETWORK_LITECOIN_TESTNET":    40,
		"NETWORK_TRON_MAINNET":        64,
		"NETWORK_TRON_TESTNET":        65,
		"NETWORK_ETHEREUM_GOERLI":     66,
		"NETWORK_DOGECOIN_MAINNET":    56,
		"NETWORK_DOGECOIN_TESTNET":    57,
		"NETWORK_BSC_MAINNET":         70,
		"NETWORK_BSC_TESTNET":         71,
		"NETWORK_AVACCHAIN_MAINNET":   72,
		"NETWORK_AVACCHAIN_TESTNET":   73,
		"NETWORK_POLYGON_MAINNET":     78,
		"NETWORK_POLYGON_TESTNET":     79,
		"NETWORK_OPTIMISM_MAINNET":    86,
		"NETWORK_OPTIMISM_TESTNET":    87,
		"NETWORK_ARBITRUM_MAINNET":    91,
		"NETWORK_ARBITRUM_TESTNET":    92,
		"NETWORK_APTOS_MAINNET":       103,
		"NETWORK_APTOS_TESTNET":       104,
		"NETWORK_FANTOM_MAINNET":      111,
		"NETWORK_FANTOM_TESTNET":      112,
		"NETWORK_BASE_MAINNET":        123,
		"NETWORK_BASE_GOERLI":         125,
		"NETWORK_ETHEREUM_HOLESKY":    136,
		"NETWORK_STORY_MAINNET":       140,
	}
)

func (x Network) Enum() *Network {
	p := new(Network)
	*p = x
	return p
}

func (x Network) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Network) Descriptor() protoreflect.EnumDescriptor {
	return file_coinbase_c3_common_common_proto_enumTypes[1].Descriptor()
}

func (Network) Type() protoreflect.EnumType {
	return &file_coinbase_c3_common_common_proto_enumTypes[1]
}

func (x Network) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Network.Descriptor instead.
func (Network) EnumDescriptor() ([]byte, []int) {
	return file_coinbase_c3_common_common_proto_rawDescGZIP(), []int{1}
}

var File_coinbase_c3_common_common_proto protoreflect.FileDescriptor

var file_coinbase_c3_common_common_proto_rawDesc = []byte{
	0x0a, 0x1f, 0x63, 0x6f, 0x69, 0x6e, 0x62, 0x61, 0x73, 0x65, 0x2f, 0x63, 0x33, 0x2f, 0x63, 0x6f,
	0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x12, 0x12, 0x63, 0x6f, 0x69, 0x6e, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x63, 0x33, 0x2e, 0x63,
	0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2a, 0x9f, 0x03, 0x0a, 0x0a, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x63,
	0x68, 0x61, 0x69, 0x6e, 0x12, 0x16, 0x0a, 0x12, 0x42, 0x4c, 0x4f, 0x43, 0x4b, 0x43, 0x48, 0x41,
	0x49, 0x4e, 0x5f, 0x55, 0x4e, 0x4b, 0x4e, 0x4f, 0x57, 0x4e, 0x10, 0x00, 0x12, 0x15, 0x0a, 0x11,
	0x42, 0x4c, 0x4f, 0x43, 0x4b, 0x43, 0x48, 0x41, 0x49, 0x4e, 0x5f, 0x53, 0x4f, 0x4c, 0x41, 0x4e,
	0x41, 0x10, 0x0b, 0x12, 0x16, 0x0a, 0x12, 0x42, 0x4c, 0x4f, 0x43, 0x4b, 0x43, 0x48, 0x41, 0x49,
	0x4e, 0x5f, 0x42, 0x49, 0x54, 0x43, 0x4f, 0x49, 0x4e, 0x10, 0x10, 0x12, 0x17, 0x0a, 0x13, 0x42,
	0x4c, 0x4f, 0x43, 0x4b, 0x43, 0x48, 0x41, 0x49, 0x4e, 0x5f, 0x45, 0x54, 0x48, 0x45, 0x52, 0x45,
	0x55, 0x4d, 0x10, 0x11, 0x12, 0x1a, 0x0a, 0x16, 0x42, 0x4c, 0x4f, 0x43, 0x4b, 0x43, 0x48, 0x41,
	0x49, 0x4e, 0x5f, 0x42, 0x49, 0x54, 0x43, 0x4f, 0x49, 0x4e, 0x43, 0x41, 0x53, 0x48, 0x10, 0x12,
	0x12, 0x17, 0x0a, 0x13, 0x42, 0x4c, 0x4f, 0x43, 0x4b, 0x43, 0x48, 0x41, 0x49, 0x4e, 0x5f, 0x4c,
	0x49, 0x54, 0x45, 0x43, 0x4f, 0x49, 0x4e, 0x10, 0x13, 0x12, 0x17, 0x0a, 0x13, 0x42, 0x4c, 0x4f,
	0x43, 0x4b, 0x43, 0x48, 0x41, 0x49, 0x4e, 0x5f, 0x44, 0x4f, 0x47, 0x45, 0x43, 0x4f, 0x49, 0x4e,
	0x10, 0x1a, 0x12, 0x13, 0x0a, 0x0f, 0x42, 0x4c, 0x4f, 0x43, 0x4b, 0x43, 0x48, 0x41, 0x49, 0x4e,
	0x5f, 0x54, 0x52, 0x4f, 0x4e, 0x10, 0x1e, 0x12, 0x12, 0x0a, 0x0e, 0x42, 0x4c, 0x4f, 0x43, 0x4b,
	0x43, 0x48, 0x41, 0x49, 0x4e, 0x5f, 0x42, 0x53, 0x43, 0x10, 0x1f, 0x12, 0x18, 0x0a, 0x14, 0x42,
	0x4c, 0x4f, 0x43, 0x4b, 0x43, 0x48, 0x41, 0x49, 0x4e, 0x5f, 0x41, 0x56, 0x41, 0x43, 0x43, 0x48,
	0x41, 0x49, 0x4e, 0x10, 0x20, 0x12, 0x16, 0x0a, 0x12, 0x42, 0x4c, 0x4f, 0x43, 0x4b, 0x43, 0x48,
	0x41, 0x49, 0x4e, 0x5f, 0x50, 0x4f, 0x4c, 0x59, 0x47, 0x4f, 0x4e, 0x10, 0x23, 0x12, 0x17, 0x0a,
	0x13, 0x42, 0x4c, 0x4f, 0x43, 0x4b, 0x43, 0x48, 0x41, 0x49, 0x4e, 0x5f, 0x4f, 0x50, 0x54, 0x49,
	0x4d, 0x49, 0x53, 0x4d, 0x10, 0x27, 0x12, 0x17, 0x0a, 0x13, 0x42, 0x4c, 0x4f, 0x43, 0x4b, 0x43,
	0x48, 0x41, 0x49, 0x4e, 0x5f, 0x41, 0x52, 0x42, 0x49, 0x54, 0x52, 0x55, 0x4d, 0x10, 0x29, 0x12,
	0x14, 0x0a, 0x10, 0x42, 0x4c, 0x4f, 0x43, 0x4b, 0x43, 0x48, 0x41, 0x49, 0x4e, 0x5f, 0x41, 0x50,
	0x54, 0x4f, 0x53, 0x10, 0x2f, 0x12, 0x15, 0x0a, 0x11, 0x42, 0x4c, 0x4f, 0x43, 0x4b, 0x43, 0x48,
	0x41, 0x49, 0x4e, 0x5f, 0x46, 0x41, 0x4e, 0x54, 0x4f, 0x4d, 0x10, 0x33, 0x12, 0x13, 0x0a, 0x0f,
	0x42, 0x4c, 0x4f, 0x43, 0x4b, 0x43, 0x48, 0x41, 0x49, 0x4e, 0x5f, 0x42, 0x41, 0x53, 0x45, 0x10,
	0x38, 0x12, 0x14, 0x0a, 0x10, 0x42, 0x4c, 0x4f, 0x43, 0x4b, 0x43, 0x48, 0x41, 0x49, 0x4e, 0x5f,
	0x53, 0x54, 0x4f, 0x52, 0x59, 0x10, 0x3c, 0x2a, 0xd5, 0x07, 0x0a, 0x07, 0x4e, 0x65, 0x74, 0x77,
	0x6f, 0x72, 0x6b, 0x12, 0x13, 0x0a, 0x0f, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x55,
	0x4e, 0x4b, 0x4e, 0x4f, 0x57, 0x4e, 0x10, 0x00, 0x12, 0x1a, 0x0a, 0x16, 0x4e, 0x45, 0x54, 0x57,
	0x4f, 0x52, 0x4b, 0x5f, 0x53, 0x4f, 0x4c, 0x41, 0x4e, 0x41, 0x5f, 0x4d, 0x41, 0x49, 0x4e, 0x4e,
	0x45, 0x54, 0x10, 0x16, 0x12, 0x1a, 0x0a, 0x16, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f,
	0x53, 0x4f, 0x4c, 0x41, 0x4e, 0x41, 0x5f, 0x54, 0x45, 0x53, 0x54, 0x4e, 0x45, 0x54, 0x10, 0x17,
	0x12, 0x1b, 0x0a, 0x17, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x42, 0x49, 0x54, 0x43,
	0x4f, 0x49, 0x4e, 0x5f, 0x4d, 0x41, 0x49, 0x4e, 0x4e, 0x45, 0x54, 0x10, 0x21, 0x12, 0x1b, 0x0a,
	0x17, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x42, 0x49, 0x54, 0x43, 0x4f, 0x49, 0x4e,
	0x5f, 0x54, 0x45, 0x53, 0x54, 0x4e, 0x45, 0x54, 0x10, 0x22, 0x12, 0x1c, 0x0a, 0x18, 0x4e, 0x45,
	0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x45, 0x54, 0x48, 0x45, 0x52, 0x45, 0x55, 0x4d, 0x5f, 0x4d,
	0x41, 0x49, 0x4e, 0x4e, 0x45, 0x54, 0x10, 0x23, 0x12, 0x1c, 0x0a, 0x18, 0x4e, 0x45, 0x54, 0x57,
	0x4f, 0x52, 0x4b, 0x5f, 0x45, 0x54, 0x48, 0x45, 0x52, 0x45, 0x55, 0x4d, 0x5f, 0x54, 0x45, 0x53,
	0x54, 0x4e, 0x45, 0x54, 0x10, 0x24, 0x12, 0x1f, 0x0a, 0x1b, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52,
	0x4b, 0x5f, 0x42, 0x49, 0x54, 0x43, 0x4f, 0x49, 0x4e, 0x43, 0x41, 0x53, 0x48, 0x5f, 0x4d, 0x41,
	0x49, 0x4e, 0x4e, 0x45, 0x54, 0x10, 0x25, 0x12, 0x1f, 0x0a, 0x1b, 0x4e, 0x45, 0x54, 0x57, 0x4f,
	0x52, 0x4b, 0x5f, 0x42, 0x49, 0x54, 0x43, 0x4f, 0x49, 0x4e, 0x43, 0x41, 0x53, 0x48, 0x5f, 0x54,
	0x45, 0x53, 0x54, 0x4e, 0x45, 0x54, 0x10, 0x26, 0x12, 0x1c, 0x0a, 0x18, 0x4e, 0x45, 0x54, 0x57,
	0x4f, 0x52, 0x4b, 0x5f, 0x4c, 0x49, 0x54, 0x45, 0x43, 0x4f, 0x49, 0x4e, 0x5f, 0x4d, 0x41, 0x49,
	0x4e, 0x4e, 0x45, 0x54, 0x10, 0x27, 0x12, 0x1c, 0x0a, 0x18, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52,
	0x4b, 0x5f, 0x4c, 0x49, 0x54, 0x45, 0x43, 0x4f, 0x49, 0x4e, 0x5f, 0x54, 0x45, 0x53, 0x54, 0x4e,
	0x45, 0x54, 0x10, 0x28, 0x12, 0x18, 0x0a, 0x14, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f,
	0x54, 0x52, 0x4f, 0x4e, 0x5f, 0x4d, 0x41, 0x49, 0x4e, 0x4e, 0x45, 0x54, 0x10, 0x40, 0x12, 0x18,
	0x0a, 0x14, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x54, 0x52, 0x4f, 0x4e, 0x5f, 0x54,
	0x45, 0x53, 0x54, 0x4e, 0x45, 0x54, 0x10, 0x41, 0x12, 0x1b, 0x0a, 0x17, 0x4e, 0x45, 0x54, 0x57,
	0x4f, 0x52, 0x4b, 0x5f, 0x45, 0x54, 0x48, 0x45, 0x52, 0x45, 0x55, 0x4d, 0x5f, 0x47, 0x4f, 0x45,
	0x52, 0x4c, 0x49, 0x10, 0x42, 0x12, 0x1c, 0x0a, 0x18, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b,
	0x5f, 0x44, 0x4f, 0x47, 0x45, 0x43, 0x4f, 0x49, 0x4e, 0x5f, 0x4d, 0x41, 0x49, 0x4e, 0x4e, 0x45,
	0x54, 0x10, 0x38, 0x12, 0x1c, 0x0a, 0x18, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x44,
	0x4f, 0x47, 0x45, 0x43, 0x4f, 0x49, 0x4e, 0x5f, 0x54, 0x45, 0x53, 0x54, 0x4e, 0x45, 0x54, 0x10,
	0x39, 0x12, 0x17, 0x0a, 0x13, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x42, 0x53, 0x43,
	0x5f, 0x4d, 0x41, 0x49, 0x4e, 0x4e, 0x45, 0x54, 0x10, 0x46, 0x12, 0x17, 0x0a, 0x13, 0x4e, 0x45,
	0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x42, 0x53, 0x43, 0x5f, 0x54, 0x45, 0x53, 0x54, 0x4e, 0x45,
	0x54, 0x10, 0x47, 0x12, 0x1d, 0x0a, 0x19, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x41,
	0x56, 0x41, 0x43, 0x43, 0x48, 0x41, 0x49, 0x4e, 0x5f, 0x4d, 0x41, 0x49, 0x4e, 0x4e, 0x45, 0x54,
	0x10, 0x48, 0x12, 0x1d, 0x0a, 0x19, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x41, 0x56,
	0x41, 0x43, 0x43, 0x48, 0x41, 0x49, 0x4e, 0x5f, 0x54, 0x45, 0x53, 0x54, 0x4e, 0x45, 0x54, 0x10,
	0x49, 0x12, 0x1b, 0x0a, 0x17, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x50, 0x4f, 0x4c,
	0x59, 0x47, 0x4f, 0x4e, 0x5f, 0x4d, 0x41, 0x49, 0x4e, 0x4e, 0x45, 0x54, 0x10, 0x4e, 0x12, 0x1b,
	0x0a, 0x17, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x50, 0x4f, 0x4c, 0x59, 0x47, 0x4f,
	0x4e, 0x5f, 0x54, 0x45, 0x53, 0x54, 0x4e, 0x45, 0x54, 0x10, 0x4f, 0x12, 0x1c, 0x0a, 0x18, 0x4e,
	0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x4f, 0x50, 0x54, 0x49, 0x4d, 0x49, 0x53, 0x4d, 0x5f,
	0x4d, 0x41, 0x49, 0x4e, 0x4e, 0x45, 0x54, 0x10, 0x56, 0x12, 0x1c, 0x0a, 0x18, 0x4e, 0x45, 0x54,
	0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x4f, 0x50, 0x54, 0x49, 0x4d, 0x49, 0x53, 0x4d, 0x5f, 0x54, 0x45,
	0x53, 0x54, 0x4e, 0x45, 0x54, 0x10, 0x57, 0x12, 0x1c, 0x0a, 0x18, 0x4e, 0x45, 0x54, 0x57, 0x4f,
	0x52, 0x4b, 0x5f, 0x41, 0x52, 0x42, 0x49, 0x54, 0x52, 0x55, 0x4d, 0x5f, 0x4d, 0x41, 0x49, 0x4e,
	0x4e, 0x45, 0x54, 0x10, 0x5b, 0x12, 0x1c, 0x0a, 0x18, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b,
	0x5f, 0x41, 0x52, 0x42, 0x49, 0x54, 0x52, 0x55, 0x4d, 0x5f, 0x54, 0x45, 0x53, 0x54, 0x4e, 0x45,
	0x54, 0x10, 0x5c, 0x12, 0x19, 0x0a, 0x15, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x41,
	0x50, 0x54, 0x4f, 0x53, 0x5f, 0x4d, 0x41, 0x49, 0x4e, 0x4e, 0x45, 0x54, 0x10, 0x67, 0x12, 0x19,
	0x0a, 0x15, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x41, 0x50, 0x54, 0x4f, 0x53, 0x5f,
	0x54, 0x45, 0x53, 0x54, 0x4e, 0x45, 0x54, 0x10, 0x68, 0x12, 0x1a, 0x0a, 0x16, 0x4e, 0x45, 0x54,
	0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x46, 0x41, 0x4e, 0x54, 0x4f, 0x4d, 0x5f, 0x4d, 0x41, 0x49, 0x4e,
	0x4e, 0x45, 0x54, 0x10, 0x6f, 0x12, 0x1a, 0x0a, 0x16, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b,
	0x5f, 0x46, 0x41, 0x4e, 0x54, 0x4f, 0x4d, 0x5f, 0x54, 0x45, 0x53, 0x54, 0x4e, 0x45, 0x54, 0x10,
	0x70, 0x12, 0x18, 0x0a, 0x14, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x42, 0x41, 0x53,
	0x45, 0x5f, 0x4d, 0x41, 0x49, 0x4e, 0x4e, 0x45, 0x54, 0x10, 0x7b, 0x12, 0x17, 0x0a, 0x13, 0x4e,
	0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x42, 0x41, 0x53, 0x45, 0x5f, 0x47, 0x4f, 0x45, 0x52,
	0x4c, 0x49, 0x10, 0x7d, 0x12, 0x1d, 0x0a, 0x18, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f,
	0x45, 0x54, 0x48, 0x45, 0x52, 0x45, 0x55, 0x4d, 0x5f, 0x48, 0x4f, 0x4c, 0x45, 0x53, 0x4b, 0x59,
	0x10, 0x88, 0x01, 0x12, 0x1a, 0x0a, 0x15, 0x4e, 0x45, 0x54, 0x57, 0x4f, 0x52, 0x4b, 0x5f, 0x53,
	0x54, 0x4f, 0x52, 0x59, 0x5f, 0x4d, 0x41, 0x49, 0x4e, 0x4e, 0x45, 0x54, 0x10, 0x8c, 0x01, 0x42,
	0x3c, 0x5a, 0x3a, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x63, 0x6f,
	0x69, 0x6e, 0x62, 0x61, 0x73, 0x65, 0x2f, 0x63, 0x68, 0x61, 0x69, 0x6e, 0x73, 0x74, 0x6f, 0x72,
	0x61, 0x67, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2f, 0x63, 0x6f, 0x69, 0x6e, 0x62,
	0x61, 0x73, 0x65, 0x2f, 0x63, 0x33, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x62, 0x06, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_coinbase_c3_common_common_proto_rawDescOnce sync.Once
	file_coinbase_c3_common_common_proto_rawDescData = file_coinbase_c3_common_common_proto_rawDesc
)

func file_coinbase_c3_common_common_proto_rawDescGZIP() []byte {
	file_coinbase_c3_common_common_proto_rawDescOnce.Do(func() {
		file_coinbase_c3_common_common_proto_rawDescData = protoimpl.X.CompressGZIP(file_coinbase_c3_common_common_proto_rawDescData)
	})
	return file_coinbase_c3_common_common_proto_rawDescData
}

var file_coinbase_c3_common_common_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_coinbase_c3_common_common_proto_goTypes = []interface{}{
	(Blockchain)(0), // 0: coinbase.c3.common.Blockchain
	(Network)(0),    // 1: coinbase.c3.common.Network
}
var file_coinbase_c3_common_common_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_coinbase_c3_common_common_proto_init() }
func file_coinbase_c3_common_common_proto_init() {
	if File_coinbase_c3_common_common_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_coinbase_c3_common_common_proto_rawDesc,
			NumEnums:      2,
			NumMessages:   0,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_coinbase_c3_common_common_proto_goTypes,
		DependencyIndexes: file_coinbase_c3_common_common_proto_depIdxs,
		EnumInfos:         file_coinbase_c3_common_common_proto_enumTypes,
	}.Build()
	File_coinbase_c3_common_common_proto = out.File
	file_coinbase_c3_common_common_proto_rawDesc = nil
	file_coinbase_c3_common_common_proto_goTypes = nil
	file_coinbase_c3_common_common_proto_depIdxs = nil
}
