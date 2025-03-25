package oo

type Bit64 uint64

func (tb *Bit64) SetBit(bits ...uint) {
	for _, bit := range bits {
		*tb |= (1 << bit)
	}
}
func (tb *Bit64) ClearBit(bits ...uint) {
	for _, bit := range bits {
		*tb &^= (1 << bit)
	}
}
func (tb *Bit64) Test(bits ...uint) bool {
	for _, bit := range bits {
		if (*tb & (1 << bit)) != (1 << bit) {
			return false
		}
	}
	return true
}
func (tb *Bit64) Equ(bits ...uint) bool {
	bv := *tb
	for _, bit := range bits {
		if bv&(1<<bit) != (1 << bit) {
			return false
		}
		bv &^= (1 << bit)
	}
	return (bv == 0)
}
func (tb *Bit64) TestReset(bits ...uint) bool {
	ok := tb.Test(bits...)
	*tb = 0
	return ok
}
func (tb *Bit64) EquReset(bits ...uint) bool {
	ok := tb.Equ(bits...)
	*tb = 0
	return ok
}
func (tb *Bit64) Reset() {
	*tb = 0
}

type Bits struct {
	nbit uint
	bval []uint64
}

func CreateBits(nbit uint) (tb *Bits) {
	tb = &Bits{}
	tb.nbit = nbit
	tb.bval = make([]uint64, (nbit+63)/64)
	return
}
func (tb *Bits) SetBit(bits ...uint) {
	for _, bit := range bits {
		if bit < tb.nbit {
			tb.bval[bit/64] |= (1 << (bit % 64))
		}
	}
}
func (tb *Bits) ClearBit(bits ...uint) {
	for _, bit := range bits {
		if bit < tb.nbit {
			tb.bval[bit/64] &^= (1 << (bit % 64))
		}
	}
}
func (tb *Bits) Test(bits ...uint) bool {
	for _, bit := range bits {
		if bit >= tb.nbit {
			return false
		}
		if (tb.bval[bit/64] & (1 << (bit % 64))) != (1 << (bit % 64)) {
			return false
		}
	}
	return true
}
func (tb *Bits) Equ(bits ...uint) bool {
	bv := make([]uint64, len(tb.bval))
	copy(bv, tb.bval)
	for _, bit := range bits {
		if bit >= tb.nbit {
			return false
		}
		if bv[bit/64]&(1<<(bit%64)) != (1 << (bit % 64)) {
			return false
		}
		bv[bit/64] &^= (1 << (bit % 64))
	}
	for i := 0; i < len(bv); i++ {
		if bv[i] != 0 {
			return false
		}
	}
	return true
}
func (tb *Bits) TestReset(bits ...uint) bool {
	ok := tb.Test(bits...)
	tb.Reset()
	return ok
}
func (tb *Bits) EquReset(bits ...uint) bool {
	ok := tb.Equ(bits...)
	tb.Reset()
	return ok
}
func (tb *Bits) Reset() {
	for i := 0; i < len(tb.bval); i++ {
		tb.bval[i] = 0
	}
}
