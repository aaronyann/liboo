package oo

import (
	"testing"
)

func TestRpcReturn(t *testing.T) {
	reqmsg := &RpcMsg{}
	// rsp, err := RpcReturn(reqmsg, NewRpcError("E_AUTH_CODE", "Wrong verification code"))
	rsp, err := RpcReturn(reqmsg, NewRpcError("E_AUTH_CODE", "test format %s", "Wrong verification code"))
	if nil != err {
		t.Fatal(err)
	}
	t.Logf("%s", JsonData(rsp))
}

func TestJsonUnmarshalValidate(t *testing.T) {
	{
		var (
			val struct{}
			buf = []byte(``)
		)
		err := JsonUnmarshalValidate(buf, &val)

		t.Log(err)
	}

	{
		var (
			val struct{}
			buf = []byte(`{"name":"test1"}`)
		)
		err := JsonUnmarshalValidate(buf, &val)

		t.Log(err)
	}

	{
		var (
			val struct {
				Name string `json:"name" validate:"gt=4"`
			}
			buf = []byte(``)
		)
		err := JsonUnmarshalValidate(buf, &val)

		t.Log(err)
	}

	{
		var (
			val struct {
				Name string `json:"name" validate:"gt=4"`
			}
			buf = []byte(`{"name":"test1"}`)
		)
		err := JsonUnmarshalValidate(buf, &val)

		t.Log(err)
	}
}

// func TestStringsUniq(t *testing.T) {
// 	ss := []string{
// 		"aa", "bb", "cc", "bb", "dd", "ee",
// 	}
// 	ss = StringsUniq(ss, []string{"dd"})
// 	t.Log(ss)
// }

func TestValidateStruct(t *testing.T) {
	type GTEIntStruct struct {
		A int `validate:"gte=0"`
	}
	type GTIntStruct struct {
		B int `validate:"gt=0"`
	}
	type LTEIntStruct struct {
		C int `validate:"lte=0"`
	}
	type LTIntStruct struct {
		D int `validate:"lt=0"`
	}
	type DynimicStringLengthStruct struct {
		E string `validate:"min=0,max=6"`
	}
	type FixedStringLengthStruct struct {
		F string `validate:"min=6,max=6"`
	}
	type FieldComparisonStruct struct {
		G1 int `validate:"gte=0"`
		G2 int `validate:"gtefield=G1"`
	}

	tests := []struct {
		a       interface{}
		noError bool
	}{
		{a: &GTEIntStruct{1}, noError: true},
		{a: &GTEIntStruct{0}, noError: true},
		{a: &GTEIntStruct{-1}, noError: false},

		{a: &GTIntStruct{1}, noError: true},
		{a: &GTIntStruct{0}, noError: false},
		{a: &GTIntStruct{-1}, noError: false},

		{a: &LTEIntStruct{1}, noError: false},
		{a: &LTEIntStruct{0}, noError: true},
		{a: &LTEIntStruct{-1}, noError: true},

		{a: &LTIntStruct{1}, noError: false},
		{a: &LTIntStruct{0}, noError: false},
		{a: &LTIntStruct{-1}, noError: true},

		{a: &DynimicStringLengthStruct{""}, noError: true},
		{a: &DynimicStringLengthStruct{"valid"}, noError: true},
		{a: &DynimicStringLengthStruct{"invalid"}, noError: false},

		{a: &FixedStringLengthStruct{""}, noError: false},
		{a: &FixedStringLengthStruct{"valid2"}, noError: true},
		{a: &FixedStringLengthStruct{"invalid"}, noError: false},

		{a: &FieldComparisonStruct{0, 0}, noError: true},
		{a: &FieldComparisonStruct{0, 1}, noError: true},
		{a: &FieldComparisonStruct{1, 0}, noError: false},
	}
	for _, tt := range tests {
		if got := ValidateStruct(tt.a); (got == nil) != tt.noError {
			var wantNoErrDesc string
			if tt.noError {
				wantNoErrDesc = "no"
			}
			t.Errorf("ValidateStruct(%+v) failed, should be %s error, got %v", tt.a, wantNoErrDesc, got)
		}
	}
}
