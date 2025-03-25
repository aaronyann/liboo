//go test -test.run
package oo

import (
	. "github.com/donnie24680/liboo"
	"testing"
)

const DB_BANK = "db_bank_usdt_omni"
const TBL_WALLET = DB_BANK + ".tb_wallet w"
const TBL_TRANS_IN = DB_BANK + ".tb_trans_in ti"
const TBL_TRANS_OUT = DB_BANK + ".tb_trans_out to"
const TBL_TRANSFER = DB_BANK + ".tb_transferring tf"
const TBL_TRANSACTION = DB_BANK + ".tb_transaction tt"

func TestSqler1(t *testing.T) {
	sql := NewSqler().Table(TBL_TRANS_IN).LeftJoin(TBL_WALLET, "w.sno=ti.sno").
		Where("w.addr", "222222").Select("ti.*")
	t.Logf("sql=%s\n", sql)
}

func TestSqler2(t *testing.T) {
	sql := NewSqler().Table("user").
		Where("id", ">", 1).                                                  // simple where
		Where("head = 3 or rate is not null").                                // where string
		Where(map[string]interface{}{"name": "fizzday", "age": 18}).          // where object
		Where([][]interface{}{{"website", "like", "%fizz%"}, {"job", "it"}}). // multi where
		OrWhere("cash", "1000000").                                           // or where ...
		OrWhere("score", "between", []string{"50", "80"}).                    // between
		OrWhere("abc", "not between", []int{3, 9}).                           // between
		OrWhere("role", "not in", []string{"admin", "read"}).                 // in
		Group("job").                                                         // group
		Having("age_avg>1").                                                  // having
		Order("age asc").                                                     // order
		Limit(10).                                                            // limit
		Offset(1).
		Select("id, name, avg(age) as age_avg")

	t.Logf("sql=%s\n", sql)
}

func TestSqler3(t *testing.T) {
	data := []map[string]interface{}{
		{
			"aaa": 3,
			"Bbb": "str",
			"ccc": -2,
			"ddd": 1,
		},
		{
			"aaa": 4,
			"Bbb": "5",
			"ccc": 8,
			"ddd": 2,
		},
	}
	sql := NewSqler().Table("tbbl").UpdateBatch(data, []string{"aaa", "ddd"}, []string{"ccc", "bbb"})

	t.Logf("sql=%s\n", sql)
}
