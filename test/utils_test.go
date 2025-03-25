package oo

import (
	. "github.com/donnie24680/liboo"
	"testing"
)

func Test_IntToBytes(t *testing.T) {
	num := 4200000000000000000
	buf := IntToBytes(int32(num))
	str := Base58Encode(buf)
	t.Log(str)
}

func TestSqler(t *testing.T) {
	sqler := NewSqler().Table("db_mine.tb_mine_block").
		Order("create_ts DESC")
	sqlstr := sqler.Offset(int(101)).Limit(int(102)).
		Select("SQL_CALC_FOUND_ROWS *")

	t.Log(sqlstr)
}

// func TestStruct2Map(t *testing.T) {
// 	mill := DTbMill{}
// 	sqlstr := NewSqler(LowerCaseWithUnderscores).Table("db_mine.tb_mill").
// 		Insert(Struct2Map(mill))
// 	t.Log(sqlstr)
// }

func TestConfig(t *testing.T) {
	c, err := InitConfig([]string{"../../hdpool/app/etc/local.conf"}, nil)
	if err != nil {
		t.Log(err)
		return
	}
	// t.Logf("%#v\n", c)
	t.Logf("%s, %d, %v", c.String("devmail.pass"), c.Int64("logind.enable"), c.Bool("logind.enable"))
	t.Logf("%v, %v, %v", c.StringArray("nats.servers"), c.Int64Array("local.traceuids"),
		c.BoolArray("nats.mbool"))
	t.Logf("%v, %v, %v", c.StringArray("nats.servers1"), c.Int64Array("local.traceuids1"),
		c.BoolArray("nats.mbool1"))
	t.Logf("%v, %v, %v", c.StringArray("nats.servers1", []string{"hh"}), c.Int64Array("local.traceuids1", []int64{3, 2}),
		c.BoolArray("nats.mbool1", []bool{true, true, false}))

	type EmailCfg struct {
		SmtpAddr string `toml:"smtp_addr, omitzero"`
		Port     int64  `toml:"port ,omitzero"`
		Uname    string `toml:"uname,omitzero"`
		Pass     string `toml:"pass ,omitzero"`
		Ssl      int64  `toml:"ssl  ,omitzero"`
	}
	var em EmailCfg
	err = c.SessDecode("devmail", &em)
	t.Logf("%#v, err=%v\n", em, err)

	c.SetSess("aabb", &em)
	// t.Logf("%#v\n", c)

	c.SetValue("devmail.pass", "33234")
	// t.Logf("%#v\n", )

	c.SaveToml("cfg.txt")
}
