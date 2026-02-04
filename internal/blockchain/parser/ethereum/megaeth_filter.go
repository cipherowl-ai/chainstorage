package ethereum

import "strings"

// IsMegaethFilteredTransaction returns true if a transaction should be filtered out
// based on whether its from or to address is in the megaeth filtered address set.
func IsMegaethFilteredTransaction(from, to string) bool {
	from = strings.ToLower(from)
	to = strings.ToLower(to)
	if _, ok := megaethFilteredAddresses[from]; ok {
		return true
	}
	if _, ok := megaethFilteredAddresses[to]; ok {
		return true
	}
	return false
}
