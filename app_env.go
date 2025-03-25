package oo

import (
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"strings"
)

var (
	AppDefaultConfFiles []string
	AppServerName       string
)

func AppGetServerEnv(git_version string) (work_dir, server_name, server_mark, server_tag string) {
	work_dir, _ = filepath.Abs(filepath.Dir(os.Args[0]))
	work_dir += "/"

	server_name = strings.Split(filepath.Base(os.Args[0]), ".")[0]
	// hostname+server_name+pid
	server_mark = GetSvrmark(server_name)
	// for log
	server_tag = server_mark + "." + git_version

	AppDefaultConfFiles = []string{
		path.Join(work_dir, "../etc/global.conf"),
		path.Join(work_dir, "../etc/local.conf"),
	}
	AppServerName = server_name

	return
}

func AppGetMainConfig(local_debug *int64, mainconf interface{}) (err error) {
	GConfig, err = InitConfig(AppDefaultConfFiles, nil)
	if nil != err {
		return
	}
	err = GConfig.SessDecode(AppServerName, mainconf)
	if nil != err {
		return
	}

	*local_debug = GConfig.Int64("local.debug", 0)

	return
}

func AppSetGoMaxProcs(core int) {
	num_cpu := runtime.NumCPU()
	if core > 0 && core <= num_cpu {
		num_cpu = core
	}
	runtime.GOMAXPROCS(num_cpu)
}

func AppInitMysqlService(service_name string) (err error) {
	var cfg MysqlCfg

	err = GConfig.SessDecode(service_name, &cfg)
	if nil != err {
		return
	}
	GMysqlPool, err = InitMysqlPool(cfg.Host, (int32)(cfg.Port), cfg.User, cfg.Pass, "")
	if nil != err {
		return
	}

	return
}

func AppInitMysqlXormService(service_name string, init_tbs map[string][]interface{}) (err error) {
	var cfg MysqlCfg

	err = GConfig.SessDecode(service_name, &cfg)
	if nil != err {
		return
	}

	if len(init_tbs) == 0 {
		init_tbs = map[string][]interface{}{
			"": []interface{}{},
		}
	}

	for db_name, tables := range init_tbs {
		err = InitXorm(cfg.Host, cfg.Port, cfg.User, cfg.Pass, db_name)
		if nil != err {
			return
		}
		if len(tables) > 0 {
			err = engine.Sync2(tables...)
			if nil != err {
				return
			}
		}
	}

	return AppInitMysqlService(service_name)
}

func AppInitRedisService(service_name string) (err error) {
	var cfg RedisCfg

	err = GConfig.SessDecode(service_name, &cfg)
	if nil != err {
		return
	}
	GRedisPool, err = InitRedisPool(cfg.Host, (int32)(cfg.Port), cfg.Pass)
	if nil != err {
		return
	}

	return
}

func AppInitNatsService(
	service_name, server_name string,
	evt_router map[string]ReqCtxHandler,
	msg_router map[string]ReqCtxHandler, def_fn interface{}) (ret *NatsService, err error) {
	var cfg NatsCfg

	err = GConfig.SessDecode(service_name, &cfg)
	if nil != err {
		return
	}

	ret, err = InitNatsService(cfg.Mservers, cfg.Servers, server_name)
	if nil != err {
		return
	}

	for cmd, fn := range evt_router {
		ret.SubEventHandle(cmd, fn)
	}

	if nil != def_fn {
		ret.SubDefaultHandle(def_fn)
	}
	for cmd, fn := range msg_router {
		ret.SubMsgHandle(cmd, fn)
	}

	return
}

func AppWaitForSignalExec(fn func(), sigs ...os.Signal) {
	notify := make(chan os.Signal, 1)
	signal.Notify(notify, sigs...)

	select {
	case <-notify:
		if nil != fn {
			fn()
		}
	}
}

func AppExit() {
	os.Exit(0)
}
