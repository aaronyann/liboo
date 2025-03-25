package oo

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Sqler is data mapper struct
type Sqler struct {
	table      string          // table
	fields     string          // fields
	where      [][]interface{} // where
	order      string          // order
	limit      int             // limit
	offset     int             // offset
	join       [][]interface{} // join
	distinct   bool            // distinct
	count      string          // count
	sum        string          // sum
	avg        string          // avg
	max        string          // max
	min        string          // min
	group      string          // group
	having     string          // having
	data       interface{}     // data
	inc_fields []string        //a = a + x
	translate  func(string) string
	optype     string // select/insert/update/delete
	ignore     string //for insert IGNORE
}

// ParseStr 转换为string
func utils_ParseStr(data interface{}) string {
	switch data.(type) {
	case time.Time:
		return data.(time.Time).Format("'2006-01-02 15:04:05'")
	case int, int64, uint, uint64:
		return fmt.Sprint(data)
	default:
		return "'" + strings.Replace(fmt.Sprint(strings.Replace(fmt.Sprint(data), "\\", `\\`, -1)), "'", `\'`, -1) + "'"
	}
}

// Implode : 字符串转数组, 接受混合类型, 最终输出的是字符串类型
func utils_Implode(data interface{}, glue string) string {
	var tmp []string

	if pi, ok := ToSlice(data); ok {
		for _, item := range pi {
			tmp = append(tmp, utils_ParseStr(item))
		}
	} else {
		return data.(string)
	}

	return strings.Join(tmp, glue)
}

func utils_RetStr(s string, e error) string {
	return s
}

func utils_Struct2Map(obj interface{}) (data map[string]interface{}) {
	t := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj)
	for t.Kind() == reflect.Ptr {
		v = v.Elem()
		t = v.Type()
	}

	if t.Kind() == reflect.Map {
		data, _ = v.Interface().(map[string]interface{})
		return
	}

	data = make(map[string]interface{})
	for i := 0; i < t.NumField(); i++ {
		sqltag := t.Field(i).Tag.Get("sqler")
		if sqltag == "-" || sqltag == "skips" {
			continue
		}
		field_name := t.Field(i).Name
		dbtag := t.Field(i).Tag.Get("db")
		tag_field_name := strings.Split(dbtag, ",")[0]
		if tag_field_name != "" {
			field_name = tag_field_name
		}
		data[field_name] = v.Field(i).Interface()
	}
	return data
}

//-------------------------------------

// Table : select table
func (dba *Sqler) Table(table string) *Sqler {
	dba.table = table
	return dba
}

// Group : select group by
func (dba *Sqler) Group(group string) *Sqler {
	dba.group = group
	return dba
}

// Having : select having
func (dba *Sqler) Having(having string) *Sqler {
	dba.having = having
	return dba
}

// Order : select order by
func (dba *Sqler) Order(order string) *Sqler {
	dba.order = order
	return dba
}

// Limit : select limit
func (dba *Sqler) Limit(limit int) *Sqler {
	dba.limit = limit
	return dba
}

// Offset : select offset
func (dba *Sqler) Offset(offset int) *Sqler {
	dba.offset = offset
	return dba
}

// Page : select page
func (dba *Sqler) Page(page int) *Sqler {
	dba.offset = (page - 1) * dba.limit
	return dba
}

// Where : query or execute where condition, the relation is and
func (dba *Sqler) Where(args ...interface{}) *Sqler {
	// 如果只传入一个参数, 则可能是字符串、一维对象、二维数组

	// 重新组合为长度为3的数组, 第一项为关系(and/or), 第二项为具体传入的参数 []interface{}
	w := []interface{}{"and", args}

	dba.where = append(dba.where, w)

	return dba
}

// OrWhere : like where , but the relation is or,
func (dba *Sqler) OrWhere(args ...interface{}) *Sqler {
	w := []interface{}{"or", args}
	dba.where = append(dba.where, w)

	return dba
}

// Join : select join query
func (dba *Sqler) Join(args ...interface{}) *Sqler {
	//dba.parseJoin(args, "INNER")
	dba.join = append(dba.join, []interface{}{"INNER", args})

	return dba
}

// LeftJoin : like join , the relation is left
func (dba *Sqler) LeftJoin(args ...interface{}) *Sqler {
	//dba.parseJoin(args, "LEFT")
	dba.join = append(dba.join, []interface{}{"LEFT", args})

	return dba
}

// RightJoin : like join , the relation is right
func (dba *Sqler) RightJoin(args ...interface{}) *Sqler {
	//dba.parseJoin(args, "RIGHT")
	dba.join = append(dba.join, []interface{}{"RIGHT", args})

	return dba
}

// Distinct : select distinct
func (dba *Sqler) Distinct() *Sqler {
	dba.distinct = true

	return dba
}

func (dba *Sqler) Ignore() *Sqler {
	dba.ignore = "ignore "

	return dba
}

//------------------------------------------------------------------------------------

// BuildQuery : build query string
func (dba *Sqler) BuildQuery() (string, error) {
	// 聚合
	unionArr := []string{
		dba.count,
		dba.sum,
		dba.avg,
		dba.max,
		dba.min,
	}
	var union string
	for _, item := range unionArr {
		if item != "" {
			union = item
			break
		}
	}
	// distinct
	distinct := If(dba.distinct, "distinct ", "")
	// fields
	fields := If(dba.fields == "", "*", dba.fields).(string)
	// table
	table := dba.table
	// join
	parseJoin, err := dba.parseJoin()
	if err != nil {
		return "", err
	}
	join := parseJoin
	// where
	// beforeParseWhereData = dba.where
	parseWhere, err := dba.parseWhere(dba.where)
	if err != nil {
		return "", err
	}
	where := If(parseWhere == "", "", " WHERE "+parseWhere).(string)
	// group
	group := If(dba.group == "", "", " GROUP BY "+dba.group).(string)
	// having
	having := If(dba.having == "", "", " HAVING "+dba.having).(string)
	// order
	order := If(dba.order == "", "", " ORDER BY "+dba.order).(string)
	// limit
	limit := If(dba.limit == 0, "", " LIMIT "+strconv.Itoa(dba.limit))
	// offset
	offset := If(dba.offset == 0, "", " OFFSET "+strconv.Itoa(dba.offset))

	//sqlstr := "select " + fields + " from " + table + " " + where + " " + order + " " + limit + " " + offset
	sqlstr := fmt.Sprintf("SELECT %s%s FROM %s%s%s%s%s%s%s%s",
		distinct, If(union != "", union, fields), table, join, where, group, having, order, limit, offset)

	return sqlstr, nil
}

// BuildExecut : build execute query string
func (dba *Sqler) BuildExecut() (string, error) {
	// insert : {"name":"fizz, "website":"fizzday.net"} or {{"name":"fizz2", "website":"www.fizzday.net"}, {"name":"fizz", "website":"fizzday.net"}}}
	// update : {"name":"fizz", "website":"fizzday.net"}
	// delete : ...
	var update, insertkey, insertval, sqlstr string
	if dba.optype != "delete" {
		update, insertkey, insertval = dba.buildData()
	}

	res, err := dba.parseWhere(dba.where)
	if err != nil {
		return res, err
	}
	where := If(res == "", "", " WHERE "+res).(string)

	tableName := dba.table
	switch dba.optype {
	case "insert":
		if where == "" {
			sqlstr = fmt.Sprintf("insert %sinto %s (%s) values %s", dba.ignore, tableName, insertkey, insertval)
		} else {
			sqlstr = fmt.Sprintf("insert %sinto %s (%s) select %s where %s", dba.ignore, tableName, insertkey, insertval, where)
		}
	case "update":
		sqlstr = fmt.Sprintf("update %s set %s%s", tableName, update, where)
	case "delete":
		sqlstr = fmt.Sprintf("delete from %s%s", tableName, where)
	}

	return sqlstr, nil
}

//暂时不支持多行更新
// buildData : build inert or update data
func (dba *Sqler) buildData() (string, string, string) {
	// insert
	var dataFields []string
	var dataValues []string
	// update or delete
	var dataObj []string

	data := dba.data

	switch data.(type) {
	case string:
		dataObj = append(dataObj, data.(string))
	case []map[string]interface{}: // insert multi datas ([]map[string]interface{})
		datas, _ := data.([]map[string]interface{})
		if len(datas) == 0 {
			return "", "", ""
		}
		for key := range datas[0] {
			if InArray(key, dataFields) == false {
				dataFields = append(dataFields, key)
			}
		}
		for _, item := range datas {
			var dataValuesSub []string
			for _, key := range dataFields {
				dataValuesSub = append(dataValuesSub, utils_ParseStr(item[key]))
			}
			dataValues = append(dataValues, "("+strings.Join(dataValuesSub, ",")+")")
		}
		if dba.translate != nil {
			for i, v := range dataFields {
				dataFields[i] = dba.translate(v)
			}
		}
		//case "map[string]interface {}":
	default: // update or insert
		var mdata map[string]interface{}
		switch data.(type) {
		case map[string]interface{}:
			mdata = data.(map[string]interface{})
		default:
			mdata = utils_Struct2Map(data)
		}

		var dataValuesSub []string
		for key, val1 := range mdata {
			if dba.translate != nil {
				key = dba.translate(key)
			}
			val := utils_ParseStr(val1)
			// insert
			dataFields = append(dataFields, key)
			dataValuesSub = append(dataValuesSub, val)
			//up
			if dba.optype == "update" && InArray(key, dba.inc_fields) {
				dataObj = append(dataObj, fmt.Sprintf("%s=%s+%s", key, key, val))
			} else {
				dataObj = append(dataObj, key+"="+val)
			}
		}
		// insert
		dataValues = append(dataValues, "("+strings.Join(dataValuesSub, ",")+")")
	}

	return strings.Join(dataObj, ","), strings.Join(dataFields, ","), strings.Join(dataValues, ",")
}

// buildUnion : build union select
func (dba *Sqler) buildUnion(union, field string) (string, error) {
	unionStr := union + "(" + field + ") as " + union
	switch union {
	case "count":
		dba.count = unionStr
	case "sum":
		dba.sum = fmt.Sprintf("COALESCE(sum(%s),0) as sum", field)
	case "avg":
		dba.avg = fmt.Sprintf("COALESCE(avg(%s),0) as avg", field)
	case "max":
		dba.max = fmt.Sprintf("COALESCE(max(%s),0) as max", field)
	case "min":
		dba.min = fmt.Sprintf("COALESCE(min(%s),0) as min", field)
	}

	return dba.BuildQuery()
}

/**
 * 将where条件中的参数转换为where条件字符串
 * example: {"id",">",1}, {"age", 18}
 */
// parseParams : 将where条件中的参数转换为where条件字符串
func (dba *Sqler) parseParams(args []interface{}) (string, error) {
	paramsLength := len(args)
	argsReal := args

	// 存储当前所有数据的数组
	var paramsToArr []string

	switch paramsLength {
	case 3: // 常规3个参数:  {"id",">",1}
		if !InArray(argsReal[1], []string{"=", ">", "<", "!=", "<>", ">=", "<=",
			"like", "not like", "in", "not in", "between", "not between"}) {
			return "", errors.New("where parameter is wrong")
		}

		paramsToArr = append(paramsToArr, argsReal[0].(string))
		paramsToArr = append(paramsToArr, argsReal[1].(string))

		switch argsReal[1] {
		case "like", "not like":
			paramsToArr = append(paramsToArr, utils_ParseStr(argsReal[2]))
		case "in", "not in":
			paramsToArr = append(paramsToArr, "("+utils_Implode(argsReal[2], ",")+")")
		case "between", "not between":
			if tmpB, ok := ToSlice(argsReal[2]); ok {
				paramsToArr = append(paramsToArr, utils_ParseStr(tmpB[0])+" and "+utils_ParseStr(tmpB[1]))
			}
		default:
			paramsToArr = append(paramsToArr, utils_ParseStr(argsReal[2]))
		}
	case 2:
		paramsToArr = append(paramsToArr, argsReal[0].(string))
		paramsToArr = append(paramsToArr, "=")
		paramsToArr = append(paramsToArr, utils_ParseStr(argsReal[1]))
	}
	return strings.Join(paramsToArr, " "), nil
}

// parseJoin : parse the join paragraph
func (dba *Sqler) parseJoin() (string, error) {
	var join []interface{}
	var returnJoinArr []string
	joinArr := dba.join

	for _, join = range joinArr {
		var w string
		var ok bool
		var args []interface{}

		if len(join) != 2 {
			return "", errors.New("join conditions are wrong")
		}

		// 获取真正的where条件
		if args, ok = join[1].([]interface{}); !ok {
			return "", errors.New("join conditions are wrong")
		}

		argsLength := len(args)
		switch argsLength {
		case 1:
			w = args[0].(string)
		case 2:
			w = args[0].(string) + " ON " + args[1].(string)
		case 4:
			w = args[0].(string) + " ON " + args[1].(string) + " " + args[2].(string) + " " + args[3].(string)
		default:
			return "", errors.New("join format error")
		}

		returnJoinArr = append(returnJoinArr, " "+join[0].(string)+" JOIN "+w)
	}

	return strings.Join(returnJoinArr, " "), nil
}

// parseWhere : parse where condition
func (dba *Sqler) parseWhere(wheres [][]interface{}) (string, error) {
	// where解析后存放每一项的容器
	var where []string

	for _, args := range wheres {
		// and或者or条件
		var condition string = args[0].(string)
		// 统计当前数组中有多少个参数
		params := args[1].([]interface{})
		paramsLength := len(params)

		switch paramsLength {
		case 3: // 常规3个参数:  {"id",">",1}
			res, err := dba.parseParams(params)
			if err != nil {
				return res, err
			}
			where = append(where, condition+" "+res)

		case 2: // 常规2个参数:  {"id",1}
			res, err := dba.parseParams(params)
			if err != nil {
				return res, err
			}
			where = append(where, condition+" "+res)
		case 1: // 二维数组或字符串
			switch paramReal := params[0].(type) {
			case string:
				where = append(where, condition+" ("+paramReal+")")
			case map[string]interface{}: // 一维数组
				var whereArr []string
				for key, val := range paramReal {
					whereArr = append(whereArr, key+"="+utils_ParseStr(val))
				}
				where = append(where, condition+" ("+strings.Join(whereArr, " and ")+")")
			case [][]interface{}: // 二维数组
				var whereMore []string
				for _, arr := range paramReal { // {{"a", 1}, {"id", ">", 1}}
					whereMoreLength := len(arr)
					switch whereMoreLength {
					case 3:
						res, err := dba.parseParams(arr)
						if err != nil {
							return res, err
						}
						whereMore = append(whereMore, res)
					case 2:
						res, err := dba.parseParams(arr)
						if err != nil {
							return res, err
						}
						whereMore = append(whereMore, res)
					default:
						return "", errors.New("where data format is wrong")
					}
				}
				where = append(where, condition+" ("+strings.Join(whereMore, " and ")+")")
			// case func(): //不再支持这种嵌套方式
			default:
				return "", errors.New("where data format is wrong")
			}
		}
	}

	return strings.TrimLeft(
		strings.TrimLeft(strings.TrimLeft(
			strings.Trim(strings.Join(where, " "), " "),
			"and"), "or"),
		" "), nil
}

//-------------------------------------------------------------------------
// Reset : reset union select
func (dba *Sqler) Reset() {
	dba.table = ""
	dba.fields = ""
	dba.where = [][]interface{}{}
	dba.order = ""
	dba.limit = 0
	dba.offset = 0
	dba.join = [][]interface{}{}
	dba.distinct = false
	dba.group = ""
	dba.having = ""
	var tmp interface{}
	dba.data = tmp

	dba.count = ""
	dba.sum = ""
	dba.avg = ""
	dba.max = ""
	dba.min = ""
}

//为了链式使用，不返回错误号，若需要则自行调用BuildQuery
func (dba *Sqler) Select(args ...interface{}) string {
	if len(args) > 0 {
		// dba.Fields(args[0].(string))
		dba.fields = args[0].(string)
	}
	return utils_RetStr(dba.BuildQuery())
}

// Insert : insert data
func (dba *Sqler) Insert(data interface{}) string {
	switch data.(type) {
	case string, map[string]interface{}, []map[string]interface{}:
		dba.data = data
	default:
		dba.data = utils_Struct2Map(data)
	}
	dba.optype = "insert"
	return utils_RetStr(dba.BuildExecut())
}

func (dba *Sqler) InsertBatch(data interface{}) string {
	dba.data = data
	dba.optype = "insert"
	return utils_RetStr(dba.BuildExecut())
}

func (dba *Sqler) InsertDuplicate(data interface{}, dupKeyFields []string, args ...interface{}) string {
	s := dba.Insert(data)

	inc_fields := []string{}
	if len(args) > 0 {
		if _, ok := args[0].(string); ok {
			inc_fields = []string{args[0].(string)}
		} else {
			inc_fields = args[0].([]string)
		}
	}

	s += " on duplicate key update "
	// switch updata.(type) {
	// case string:
	// 	s += updata.(string)
	// default: //肯定是map[string]interface{}
	splits := ""
	for key, val1 := range data.(map[string]interface{}) {
		if dba.translate != nil {
			key = dba.translate(key)
		}
		if InArray(key, dupKeyFields) {
			continue
		}
		val := utils_ParseStr(val1)
		if InArray(key, inc_fields) {
			s += fmt.Sprintf("%s%s=%s+%s", splits, key, key, val)
		} else {
			s += fmt.Sprintf("%s%s=%s", splits, key, val)
		}
		splits = ", "
	}
	// }
	return s
}

// Update : update data: string, map[string]interface{}, struct
func (dba *Sqler) Update(data interface{}, args ...interface{}) string {
	if len(args) > 0 {
		if _, ok := args[0].(string); ok {
			dba.inc_fields = []string{args[0].(string)}
		} else {
			dba.inc_fields = args[0].([]string)
		}
	}
	switch data.(type) {
	case string, map[string]interface{}, []map[string]interface{}:
		dba.data = data
	default:
		dba.data = utils_Struct2Map(data)
	}

	dba.optype = "update"
	return utils_RetStr(dba.BuildExecut())
}

func (dba *Sqler) UpdateBatch(data []map[string]interface{}, keyarr interface{}, args ...interface{}) string {
	if len(args) > 0 {
		if _, ok := args[0].(string); ok {
			dba.inc_fields = []string{args[0].(string)}
		} else {
			dba.inc_fields = args[0].([]string)
		}
	}
	keystr := []string{}
	if _, ok := keyarr.(string); ok {
		keystr = []string{keyarr.(string)}
	} else {
		keystr = keyarr.([]string)
	}

	cases := map[string]string{}
	ids := ""
	for _, item := range data {
		keyval := ""

		// if dba.translate == nil {
		itval, _ := item[keystr[0]]
		keyval = utils_ParseStr(itval)
		// }

		if len(keyval) == 0 {
			fmt.Printf("what? k=%v, len=%d, item=%v\n", keyval, len(keyval), item)
			return ""
		}

		ids = ids + If(len(ids) == 0, "", ",").(string) + keyval
		for k, v := range item {
			// if dba.translate != nil {
			// 	k = dba.translate(k)
			// }
			if InArray(k, keystr) {
				continue
			}
			var ifstr string
			for _, onekey := range keystr {
				if len(ifstr) != 0 {
					ifstr += " and "
				}
				itval, _ := item[onekey]
				ifstr += fmt.Sprintf("%s=%v", onekey, utils_ParseStr(itval))
			}
			if _, ok := cases[k]; !ok {
				// cases[k] = fmt.Sprintf("case %s ", key_name)
				cases[k] = "case "
			}
			cases[k] = cases[k] + fmt.Sprintf("when %s then %s ", ifstr, utils_ParseStr(v))
		}
	}
	s := ""
	for k, v := range cases {
		if InArray(k, dba.inc_fields) {
			s = s + If(len(s) == 0, "", ", ").(string) + fmt.Sprintf("%s=%s+%send", k, k, v)
		} else {
			s = s + If(len(s) == 0, "", ", ").(string) + fmt.Sprintf("%s=%send", k, v)
		}
	}
	return fmt.Sprintf("update %s set ", dba.table) + s + fmt.Sprintf(" where %s in (%s)", keystr[0], ids)
}

// Delete : delete data
func (dba *Sqler) Delete() string {
	dba.optype = "delete"

	res, err := dba.parseWhere(dba.where)
	if err != nil {
		return ""
	}
	where := If(res == "", "", " WHERE "+res).(string)

	return fmt.Sprintf("delete from %s%s", dba.table, where)
}

// Count : select count rows
func (dba *Sqler) Count(args ...interface{}) string {
	fields := "*"
	if len(args) > 0 {
		fields = utils_ParseStr(args[0])
	}
	return utils_RetStr(dba.buildUnion("count", fields))
}

// Sum : select sum field
func (dba *Sqler) Sum(sum string) string {
	return utils_RetStr(dba.buildUnion("sum", sum))
}

// Avg : select avg field
func (dba *Sqler) Avg(avg string) string {
	return utils_RetStr(dba.buildUnion("avg", avg))
}

// Max : select max field
func (dba *Sqler) Max(max string) string {
	return utils_RetStr(dba.buildUnion("max", max))
}

// Min : select min field
func (dba *Sqler) Min(min string) string {
	return utils_RetStr(dba.buildUnion("min", min))
}

func NewSqler(args ...interface{}) *Sqler {
	dba := &Sqler{}
	if len(args) > 0 {
		dba.translate = args[0].(func(string) string)
	}
	return dba
}
