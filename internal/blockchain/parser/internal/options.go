package internal

// parseOptions is the internal state bag that concrete ParseOption values
// mutate. It is unexported so only this package can implement the
// ParseOption interface.
type parseOptions struct {
	skipScripts   bool
	skipWitnesses bool
	skipShielded  bool
}

// ParseOption configures a ParseNativeBlock / ParseBlock call. Callers
// construct options via the With* helpers; the interface is sealed so
// only this package can define new options.
type ParseOption interface {
	applyParseOption(*parseOptions)
}

type parseOptionFunc func(*parseOptions)

func (f parseOptionFunc) applyParseOption(o *parseOptions) { f(o) }

// WithSkipScripts asks the parser to omit bitcoin-family script bytes
// (ScriptSignature.{Assembly,Hex} and ScriptPublicKey.{Assembly,Hex})
// and the ethereum transaction Input field from its output. Address
// derivation that requires the script assembly (e.g. P2PK) may fall
// back to empty output when this option is set.
func WithSkipScripts() ParseOption {
	return parseOptionFunc(func(o *parseOptions) { o.skipScripts = true })
}

// WithSkipWitnesses asks the parser to omit
// BitcoinTransactionInput.TransactionInputWitnesses from its output.
func WithSkipWitnesses() ParseOption {
	return parseOptionFunc(func(o *parseOptions) { o.skipWitnesses = true })
}

// WithSkipShielded asks the bitcoin-family parser to discard zcash
// shielded transaction fields (joinsplits, vShieldedSpend,
// vShieldedOutput, vJoinSplit, saplingBundle, orchardBundle) during
// JSON decoding so no Go strings are allocated for them.
func WithSkipShielded() ParseOption {
	return parseOptionFunc(func(o *parseOptions) { o.skipShielded = true })
}

// ResolveParseOptions collapses a variadic option list into a concrete
// view that parser implementations in sibling packages can read.
func ResolveParseOptions(opts []ParseOption) ParseOptionsView {
	var p parseOptions
	for _, opt := range opts {
		if opt != nil {
			opt.applyParseOption(&p)
		}
	}
	return ParseOptionsView{p: p}
}

// ParseOptionsView is the read-only view of a resolved option set.
type ParseOptionsView struct {
	p parseOptions
}

func (v ParseOptionsView) SkipScripts() bool   { return v.p.skipScripts }
func (v ParseOptionsView) SkipWitnesses() bool { return v.p.skipWitnesses }
func (v ParseOptionsView) SkipShielded() bool  { return v.p.skipShielded }
