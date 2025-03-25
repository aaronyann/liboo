package oo

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	htmlApos = []byte("\\u0027")
	htmlAmp  = []byte("\\u0026")
	htmlLt   = []byte("\\u003c")
	htmlGt   = []byte("\\u003e")
)

func GetRealIP(r *http.Request) string {
	ip := r.RemoteAddr
	if fw := r.Header.Get("X-Forwarded-For"); fw != "" {
		ip = fw
		if ips := strings.Split(fw, ", "); len(ips) > 1 {
			ip = ips[0]
		}
	}
	return ip
}

func CheckBasicAuth(w http.ResponseWriter, r *http.Request) (uname string, upass string, bRet bool) {
	s := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
	if len(s) != 2 {
		return "", "", false
	}

	b, err := base64.StdEncoding.DecodeString(s[1])
	if err != nil {
		return "", "", false
	}

	pair := strings.SplitN(string(b), ":", 2)
	if len(pair) != 2 {
		return "", "", false
	}

	return pair[0], pair[1], true
}

func UploadHandler(w http.ResponseWriter, r *http.Request, up_path string) {
	switch r.Method {
	//POST takes the uploaded file(s) and saves it to disk.
	case "POST":
		//parse the multipart form in the request
		err := r.ParseMultipartForm(100000)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		//get a ref to the parsed multipart form
		m := r.MultipartForm

		//get the *fileheaders
		files := m.File["uploadfile"]
		for i := range files {
			//for each fileheader, get a handle to the actual file
			file, err := files[i].Open()
			defer file.Close()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			ext := filepath.Ext(files[i].Filename)

			t := time.Now()
			fname := fmt.Sprintf("%s_%09d_%06d%s", t.Format("20060102_150405_"),
				t.UnixNano()-t.Unix(), rand.Intn(1000000), ext)
			//create destination file making sure the path is writeable.
			// dst, err := os.Create("./upload/" + files[i].Filename)
			dst, err := os.Create(up_path + string(os.PathSeparator) + fname)
			defer dst.Close()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			//copy the uploaded file to the destination file
			if _, err := io.Copy(dst, file); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			fmt.Printf("uploaded %s->%s\n", files[i].Filename, up_path+fname)
		}

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func CalBasicAuthValue(username, password string) string {
	basicAuth := func() string {
		auth := username + ":" + password
		return base64.StdEncoding.EncodeToString([]byte(auth))
	}

	return "Basic " + basicAuth()
}

func HttpRequest(method, url string, header map[string]string, body []byte) (buf []byte, err error) {
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if nil != err {
		return
	}

	if nil != header {
		for k, v := range header {
			req.Header.Set(k, v)
		}
	}

	resp, err := client.Do(req)
	if nil != err {
		return
	}
	defer resp.Body.Close()

	// code = resp.StatusCode
	if http.StatusOK != resp.StatusCode && http.StatusNoContent != resp.StatusCode {
		err = NewError("resp.StatusCode %d", resp.StatusCode)
		return
	}

	buf, err = ioutil.ReadAll(resp.Body)
	// if nil != err {
	// 	return
	// }

	return
}

func HttpRequestAndParse(method, url string, header map[string]string, body []byte, val interface{}) (err error) {
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if nil != err {
		return
	}

	if nil != header {
		for k, v := range header {
			req.Header.Set(k, v)
		}
	}

	resp, err := client.Do(req)
	if nil != err {
		return
	}
	defer resp.Body.Close()

	buf, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return
	}

	if http.StatusOK != resp.StatusCode {
		err = NewError("resp.StatusCode %d. url=%s %s", resp.StatusCode, url, buf)
		return
	}

	if nil != val {
		err = JsonUnmarshal(buf, val)
		if nil != err {
			err = NewError("JsonUnmarshal err[%v] data[%s]", err, buf)
		}
	}

	return
}

func HttpRequestTimeoutAndParse(method, url string, header map[string]string, body []byte, timeout int, val interface{}) (err error) {
	client := &http.Client{Timeout: time.Duration(timeout) * time.Second}
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if nil != err {
		return
	}

	if nil != header {
		for k, v := range header {
			req.Header.Set(k, v)
		}
	}

	resp, err := client.Do(req)
	if nil != err {
		return
	}
	defer resp.Body.Close()

	buf, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return
	}

	if http.StatusOK != resp.StatusCode {
		err = NewError("resp.StatusCode %d. url=%s req[%s] rsp[%s]",
			resp.StatusCode, url, body, buf)
		return
	}

	if nil != val {
		err = JsonUnmarshal(buf, val)
		if nil != err {
			err = NewError("JsonUnmarshal err[%v] data[%s]", err, buf)
		}
	}

	return
}

func HttpRequestAndParseWithProxy(client *http.Client, method, url string, header map[string]string, body []byte, val interface{}) (err error) {
	// client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if nil != err {
		return
	}

	if nil != header {
		for k, v := range header {
			req.Header.Set(k, v)
		}
	}

	resp, err := client.Do(req)
	if nil != err {
		return
	}
	defer resp.Body.Close()

	buf, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return
	}

	if http.StatusOK != resp.StatusCode {
		err = NewError("resp.StatusCode %d. url=%s %s", resp.StatusCode, url, buf)
		return
	}

	if nil != val {
		err = JsonUnmarshal(buf, val)
		if nil != err {
			err = NewError("JsonUnmarshal err[%v] data[%s]", err, buf)
		}
	}

	return
}

func JsonEncode(s string) string {
	if !strings.ContainsAny(s, "'&<>") {
		return s
	}
	var b bytes.Buffer
	EscapeByte(&b, []byte(s))
	return b.String()
}

func EscapeByte(w io.Writer, b []byte) {
	last := 0
	for i, c := range b {
		var html []byte
		switch c {
		case '\'':
			html = htmlApos
		case '&':
			html = htmlAmp
		case '<':
			html = htmlLt
		case '>':
			html = htmlGt
		default:
			continue
		}
		w.Write(b[last:i])
		w.Write(html)
		last = i + 1
	}
	w.Write(b[last:])
}
