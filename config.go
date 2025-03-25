package oo

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/BurntSushi/toml"
)

type Config struct {
	sss  sync.Map //map[string]string
	cfgs sync.Map //map[string]interface{} //"xx.xx": []bool/int64/string
	// has_mdy bool
}

var GConfig *Config

func (c *Config) Int64(sesskey string, defs ...int64) int64 {
	// if v, ok := c.cfgs[sesskey]; ok {
	if v, ok := c.cfgs.Load(sesskey); ok {
		ret, _ := v.(int64)
		return ret
	}
	defval := int64(0)
	if len(defs) > 0 {
		defval = defs[0]
	}

	return defval
}

func (c *Config) Bool(sesskey string, defs ...bool) bool {
	// if v, ok := c.cfgs[sesskey]; ok {
	if v, ok := c.cfgs.Load(sesskey); ok {
		ret, _ := v.(bool)
		return ret
	}
	defval := bool(false)
	if len(defs) > 0 {
		defval = defs[0]
	}

	return defval
}

func (c *Config) String(sesskey string, defs ...string) string {
	// if v, ok := c.cfgs[sesskey]; ok {
	if v, ok := c.cfgs.Load(sesskey); ok {
		ret, _ := v.(string)
		return ret
	}
	defval := string("")
	if len(defs) > 0 {
		defval = defs[0]
	}
	return defval
}

func (c *Config) Int64Array(sesskey string, defs ...[]int64) []int64 {
	// if v, ok := c.cfgs[sesskey]; ok {
	if v, ok := c.cfgs.Load(sesskey); ok {
		var ret []int64
		if reflect.ValueOf(v).Kind() != reflect.Slice {
			if ret1, ok := v.(int64); ok {
				ret = []int64{ret1}
			}
		} else {
			for _, v1 := range v.([]interface{}) {
				if ret1, ok := v1.(int64); ok {
					ret = append(ret, ret1)
				}
			}
		}
		// fmt.Printf("Int64Array: %v, %v\n", v, ret)
		return ret
	}
	defval := []int64{}
	if len(defs) > 0 {
		defval = defs[0]
	}
	return defval
}

func (c *Config) BoolArray(sesskey string, defs ...[]bool) []bool {
	// if v, ok := c.cfgs[sesskey]; ok {
	if v, ok := c.cfgs.Load(sesskey); ok {
		var ret []bool
		if reflect.ValueOf(v).Kind() != reflect.Slice {
			if ret1, ok := v.(bool); ok {
				ret = []bool{ret1}
			}
		} else {
			for _, v1 := range v.([]interface{}) {
				if ret1, ok := v1.(bool); ok {
					ret = append(ret, ret1)
				}
			}
		}
		// fmt.Printf("BoolArray: %v, %v\n", v, ret)
		return ret
	}
	defval := []bool{}
	if len(defs) > 0 {
		defval = defs[0]
	}
	return defval
}

func (c *Config) StringArray(sesskey string, defs ...[]string) []string {
	// if v, ok := c.cfgs[sesskey]; ok {
	if v, ok := c.cfgs.Load(sesskey); ok {
		var ret []string
		if reflect.ValueOf(v).Kind() != reflect.Slice {
			if ret1, ok := v.(string); ok {
				ret = []string{ret1}
			}
		} else {
			for _, v1 := range v.([]interface{}) {
				if ret1, ok := v1.(string); ok {
					ret = append(ret, ret1)
				}
			}
		}
		// fmt.Printf("StringArray: %v, %v\n", v, ret)
		return ret
	}
	defval := []string{}
	if len(defs) > 0 {
		defval = defs[0]
	}
	return defval
}

func (c *Config) SessDecode(sess string, pdata interface{}) error {
	// if v, ok := c.sss[sess]; ok {
	if v, ok := c.sss.Load(sess); ok {
		vs, _ := v.(string)
		_, err := toml.Decode(vs, pdata)
		return err
	}
	return NewError("no sess: %s", sess)
}

func (c *Config) SessDecodeMap(cfgmap map[string]interface{}) error {
	for sess, pdata := range cfgmap {
		if err := c.SessDecode(sess, pdata); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) SetSess(sess string, v interface{}) {

	if reflect.TypeOf(v).Kind() == reflect.Ptr {
		v = reflect.Indirect(reflect.ValueOf(v)).Interface()
	}
	tv := reflect.TypeOf(v)
	rv := reflect.ValueOf(v)

	for i := 0; i < tv.NumField(); i++ {
		key := tv.Field(i).Name
		t := GetTagOptions(tv.Field(i).Tag, "toml")
		if t.Name != "" {
			key = t.Name
		}
		sesskey := fmt.Sprintf("%s.%s", sess, key)
		// c.cfgs[sesskey] = rv.Field(i).Interface()
		c.cfgs.Store(sesskey, rv.Field(i).Interface())
	}

	buf := new(bytes.Buffer)
	if err := toml.NewEncoder(buf).Encode(v); err == nil {
		//c.sss[sess] = buf.String()
		c.sss.Store(sess, buf.String())
	}
}

func (c *Config) SetValue(sesskey string, v interface{}) {
	// c.cfgs[sesskey] = v
	c.cfgs.Store(sesskey, v)

	// if reflect.ValueOf(v).Kind() != reflect.Slice {
	// 	c.cfgs[sesskey] = v
	// } else {
	// 	c.cfgs[sesskey] = ToSlice(v)
	// }
}

func (c *Config) SaveToml(fname string) error {
	f, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	tmp := map[string]map[string]interface{}{}
	c.cfgs.Range(func(sesskey1, v interface{}) bool {
		sesskey, _ := sesskey1.(string)
		sk := strings.Split(sesskey, ".")
		if len(sk) != 2 {
			return true
			// return NewError("cfgs more than one .")
		}
		if _, ok := tmp[sk[0]]; !ok {
			tmp[sk[0]] = make(map[string]interface{})
		}
		tmp[sk[0]][sk[1]] = v
		return true
	})

	return toml.NewEncoder(f).Encode(tmp)
}

func convet_to_cfg(c *Config, tmp map[string]map[string]interface{}) {
	buf := new(bytes.Buffer)
	for sess, ss := range tmp {
		buf.Reset()
		if err := toml.NewEncoder(buf).Encode(ss); err == nil {
			if _, loaded := c.sss.LoadOrStore(sess, buf.String()); loaded {
				LogW("duplicate session: %v", sess)
			}
		}

		for key, val := range ss {
			sesskey := fmt.Sprintf("%s.%s", sess, key)
			c.cfgs.Store(sesskey, val)
		}
	}
}
func InitConfig(cfg_files interface{}, fn func(c *Config)) (*Config, error) {

	c := &Config{
		// sss:  make(map[string]string),
		// cfgs: make(map[string]interface{}),
	}
	var cfgs []string
	if cfg, ok := cfg_files.(string); ok {
		cfgs = []string{cfg}
	} else if cfgs1, ok := cfg_files.([]string); ok {
		for _, cfg := range cfgs1 {
			cfgs = append(cfgs, cfg)
		}
	}

	f := func(c *Config) {
		c.sss = sync.Map{}
		c.cfgs = sync.Map{}

		// buf := new(bytes.Buffer)
		for _, f := range cfgs {
			tmp := map[string]map[string]interface{}{}
			_, err := toml.DecodeFile(f, &tmp)
			if err != nil {
				LogD("Not found config: %s, err=%v", f, err)
				continue
			}
			convet_to_cfg(c, tmp)
		}
	}

	f(c)

	if fn != nil {
		sig := NewSignalHandler(SigHup)
		go func() {
			for {
				select {
				case <-sig.GetChan():
					LogD("Get a SigHup.")
					f(c)

					fn(c)
				}
			}
		}()
	}

	return c, nil
}

/*
// type DTbSysconfig struct {
// 	CfgId      int64  `db:"cfg_id,omitempty" sqler:"skips"`
// 	CfgName    string `db:"cfg_name,omitempty"`
// 	CfgVal     string `db:"cfg_val,omitempty"`
// 	CfgType    string `db:"cfg_type,omitempty"`
// 	CfgComment string `db:"cfg_comment,omitempty"`
// 	UpdateTs   string `db:"update_ts,omitempty" sqler:"skips"`
// }

type SysConfig struct {
	Config
	tbname string
}

func (c *SysConfig) SaveValue(sesskey string, val interface{}) {

}

func InitSysConfig(tbname string, sync_sec int64) (*SysConfig, error) {

	c := &SysConfig{}
	c.tbname = tbname

	f := func(c *SysConfig) error {
		var err error
		var ok bool
		if GMysqlPool == nil {
			return NewError("No sql conn")
		}
		db := GMysqlPool.GetConn()
		defer GMysqlPool.UnGetConn(db)

		sql := NewSqler().Table(c.tbname).Select("*")
		var rows []DTbSysconfig
		if err = db.Select(&rows, sql); err != nil {
			return err
		}

		tmp := map[string]map[string]interface{}{}
		for _, row := range rows {
			sk := strings.Split(row.CfgName, ".")
			if len(sk) != 2 {
				continue
			}
			if _, ok = tmp[sk[0]]; !ok {
				tmp[sk[0]] = map[string]interface{}{}
			}
			switch row.CfgType {
			case "int64":
				nVal, _ := strconv.ParseInt(row.CfgVal, 10, 64)
				tmp[sk[0]][sk[1]] = nVal
			case "string":
				tmp[sk[0]][sk[1]] = row.CfgVal
			case "[]int64":
				var nArr []int64
				if JsonUnmarshal([]byte(row.CfgVal), &nArr) != nil {
					//
				}
				tmp[sk[0]][sk[1]] = nArr
			case "[]string":
				var sArr []string
				if JsonUnmarshal([]byte(row.CfgVal), &sArr) != nil {
					//
				}
				tmp[sk[0]][sk[1]] = sArr
			default:
			}
		}

		convet_to_cfg(&c.Config, tmp)
		return nil
	}

	if err := f(c); err != nil {
		return nil, err
	}

	go func() {
		for {
			<-time.After(time.Second * time.Duration(sync_sec))
			c1 := &SysConfig{}
			if err := f(c1); err != nil {
				LogW("Failed to load sysconfig. err=%v", err)
				continue
			}

		}
	}()

	return c, nil
}
*/

type SysConfig struct {
	sqlPool  *MysqlPool
	tblName  string
	keyField string
	valField string
}

var GSysConfig *SysConfig

func InitSysConfig(sql_pool *MysqlPool, table_name, key_field, val_field string) (sc *SysConfig, err error) {
	sc = &SysConfig{
		sqlPool:  sql_pool,
		tblName:  table_name,
		keyField: key_field,
		valField: val_field,
	}
	if GSysConfig == nil {
		GSysConfig = sc
	}
	return
}

// val: int64, string, []int64, []string
func (sc *SysConfig) Read(key string, pval interface{}) (err error) {
	conn := sc.sqlPool.GetConn()
	defer sc.sqlPool.UnGetConn(conn)

	sqlstr := fmt.Sprintf("select %s from %s where %s='%s'", sc.valField, sc.tblName, sc.keyField, key)
	var str string
	err = conn.Get(&str, sqlstr)
	if nil != err && ErrNoRows != err {
		err = NewError("%v sql[%s]", err, sqlstr)
		return
	}

	switch pval.(type) {
	case *bool:
		pv, _ := pval.(*bool)
		var tmpv int64
		tmpv, err = strconv.ParseInt(str, 10, 64)
		if err == nil && tmpv != 0 {
			*pv = true
		}
	case *int64:
		pv, _ := pval.(*int64)
		*pv, err = strconv.ParseInt(str, 10, 64)
	case *string:
		pv, _ := pval.(*string)
		*pv = str
	case *[]bool:
		pv, _ := pval.(*[]bool)
		var tmpv []int64
		if err = sc.Read(key, &tmpv); err == nil && len(tmpv) > 0 {
			*pv = make([]bool, len(tmpv))
			for i := 0; i < len(tmpv); i++ {
				if tmpv[i] != 0 {
					(*pv)[i] = true
				}
			}
		}
	case *[]int64:
		pv, _ := pval.(*[]int64)
		str = strings.Trim(strings.Trim(str, "["), "]")
		strs := strings.Split(str, ",")
		*pv = make([]int64, len(strs))
		for i := 0; i < len(strs); i++ {
			(*pv)[i], err = strconv.ParseInt(strs[i], 10, 64)
			if nil != err {
				return
			}
		}
	case *[]string:
		pv, _ := pval.(*[]string)
		str = strings.Trim(strings.Trim(str, "["), "]")
		strs := strings.Split(str, ",")
		*pv = make([]string, len(strs))
		for i := 0; i < len(strs); i++ {
			(*pv)[i] = strings.Trim(strs[i], "\"")
		}
	default:
		err = NewError("Not support format")
		return
	}
	return
}

func (sc *SysConfig) Int64(sesskey string, defs ...int64) (defval int64) {
	if err := sc.Read(sesskey, &defval); err != nil {
		if len(defs) > 0 {
			defval = defs[0]
		}
	}
	return
}

func (sc *SysConfig) Bool(sesskey string, defs ...bool) (defval bool) {
	if err := sc.Read(sesskey, &defval); err != nil {
		if len(defs) > 0 {
			defval = defs[0]
		}
	}
	return
}

func (sc *SysConfig) String(sesskey string, defs ...string) (defval string) {
	if err := sc.Read(sesskey, &defval); err != nil {
		if len(defs) > 0 {
			defval = defs[0]
		}
	}
	return
}

func (sc *SysConfig) Int64Array(sesskey string, defs ...[]int64) (defval []int64) {
	if err := sc.Read(sesskey, &defval); err != nil {
		if len(defs) > 0 {
			defval = defs[0]
		}
	}
	return
}

func (sc *SysConfig) BoolArray(sesskey string, defs ...[]bool) (defval []bool) {
	if err := sc.Read(sesskey, &defval); err != nil {
		if len(defs) > 0 {
			defval = defs[0]
		}
	}
	return
}

func (sc *SysConfig) StringArray(sesskey string, defs ...[]string) (defval []string) {
	if err := sc.Read(sesskey, &defval); err != nil {
		if len(defs) > 0 {
			defval = defs[0]
		}
	}
	return
}

func (sc *SysConfig) Store(key string, val interface{}) (err error) {
	conn := sc.sqlPool.GetConn()
	defer sc.sqlPool.UnGetConn(conn)

	var sval string
	switch val.(type) {
	case bool:
		v, _ := val.(bool)
		sval = "0"
		if v {
			sval = "1"
		}
	case int64, string:
		sval = fmt.Sprintf("%v", val)
	case []bool:
		v, _ := val.([]bool)
		for i := 0; i < len(v); i++ {
			if len(sval) > 0 {
				sval += ","
			}
			tmpv := "0"
			if v[i] {
				tmpv = "1"
			}
			sval += tmpv
		}
		sval = "[" + sval + "]"
	case []int64:
		v, _ := val.([]int64)
		for i := 0; i < len(v); i++ {
			if len(sval) > 0 {
				sval += ","
			}
			sval += fmt.Sprintf("%d", v[i])
		}
		sval = "[" + sval + "]"
	case []string:
		v, _ := val.([]string)
		for i := 0; i < len(v); i++ {
			if len(sval) > 0 {
				sval += ","
			}
			sval += fmt.Sprintf("\"%s\"", v[i])
		}
		sval = "[" + sval + "]"
	default:
		err = NewError("Not support format")
		return
	}
	sqlstr := fmt.Sprintf("update %s set %s='%v' where %s='%s'", sc.tblName, sc.valField, sval, sc.keyField, key)
	_, err = conn.Exec(sqlstr)
	if nil != err {
		err = NewError("%v sql[%s]", err, sqlstr)
	}

	return
}

// -vVdD -c cfgfile -l logpath
type CmdArgs = struct {
	CfgFiles []string
	LogPath  string
	Name     string
	Debug    int64
	Daemon   bool
	Version  bool
	Help     bool

	Parsed bool
}

var GCmdArgs CmdArgs

func ParseCmdArgs(ver string) (ca *CmdArgs) {
	ca = &GCmdArgs
	if ca.Parsed {
		return
	}

	cfgfile := string("")
	flag.StringVar(&cfgfile, "c", "../etc/local.conf,../etc/global.conf", "config files")
	flag.BoolVar(&ca.Daemon, "D", false, "deamon application")
	flag.StringVar(&ca.LogPath, "l", "./", "log files path")
	flag.StringVar(&ca.Name, "n", "", "this server name: name(default: exe file)")
	flag.BoolVar(&ca.Version, "v", false, "print version")
	flag.BoolVar(&ca.Version, "V", false, "print version")
	flag.Int64Var(&ca.Debug, "d", 0, "debug")
	flag.BoolVar(&ca.Help, "h", false, "help")
	flag.Parse()

	if ca.Help || ca.Version {
		fmt.Printf("Version: %s\n", ver)
		if ca.Help {
			flag.PrintDefaults()
		}
		os.Exit(0)
	}
	ca.CfgFiles = strings.Split(cfgfile, ",")
	ca.Parsed = true

	return
}
