package oo

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/smtp"
	"strings"
	"time"
)

type SmtpClient struct {
	user     string
	addr     string
	nickName string
	isSSL    bool
	auth     smtp.Auth
	timeout  time.Duration
}

type EmailCfg struct {
	SmtpAddr string `toml:"smtp_addr,omitzero"`
	Port     int64  `toml:"port,omitzero"`
	Uname    string `toml:"uname,omitzero"`
	Pass     string `toml:"pass,omitzero"`
	Ssl      int64  `toml:"ssl,omitzero"`
}

func NewSmtpClient(user, password, nickName, host string, port int64, isSsl bool) *SmtpClient {
	cli := &SmtpClient{
		user:  user,
		addr:  fmt.Sprintf("%s:%d", host, port),
		isSSL: isSsl,
		// auth:    smtp.PlainAuth("", user, password, host),
		timeout: time.Second * 30,
	}
	if password != "" {
		cli.auth = smtp.PlainAuth("", user, password, host)
	}
	if nickName == "" {
		cli.nickName = user
	} else {
		cli.nickName = nickName
	}
	return cli
}

func (cli *SmtpClient) generateEmailMsg(toUser []string, subject, content string) []byte {
	return cli.generateEmailMsgByte(toUser, subject, []byte(content))
}

func (cli *SmtpClient) generateEmailMsgByte(toUser []string, subject string, body []byte) []byte {
	msgStr := fmt.Sprintf("To: %s\r\nFrom: %s<%s>\r\nSubject: %s\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n",
		strings.Join(toUser, ","), cli.nickName, cli.user, subject)
	return append([]byte(msgStr), body...)
}

func (cli *SmtpClient) sendMailTLS(toUser []string, msg []byte) error {
	host, _, _ := net.SplitHostPort(cli.addr)
	tlsconfig := &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         host,
	}
	// dial with timeout
	conn, err := net.DialTimeout("tcp", cli.addr, cli.timeout)
	if nil != err {
		return fmt.Errorf("SmtpClient:net.DialTimeout:%v", err)
	}
	tlsConn := tls.Client(conn, tlsconfig)

	// conn, err := tls.Dial("tcp", cli.addr, tlsconfig)
	// if err != nil {
	// 	return fmt.Errorf("DialConn:%v", err)
	// }
	// client, err := smtp.NewClient(conn, host)
	client, err := smtp.NewClient(tlsConn, host)
	if err != nil {
		return fmt.Errorf("SmtpClient:generateSmtpClient:%v", err)
	}
	defer client.Close()
	if cli.auth != nil {
		if ok, _ := client.Extension("AUTH"); ok {
			if err = client.Auth(cli.auth); err != nil {
				return fmt.Errorf("SmtpClient:clientAuth:%v", err)
			}
		}
	}
	if err = client.Mail(cli.user); err != nil {
		return fmt.Errorf("SmtpClient:clientMail:%v", err)
	}

	for _, addr := range toUser {
		if err = client.Rcpt(addr); err != nil {
			return fmt.Errorf("SmtpClient:Rcpt:%v", err)
		}
	}
	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("SmtpClient:%v", err)
	}
	_, err = w.Write(msg)
	if err != nil {
		return fmt.Errorf("SmtpClient:WriterBody:%v", err)
	}
	err = w.Close()
	if err != nil {
		return fmt.Errorf("SmtpClient:CloseBody:%v", err)
	}
	return client.Quit()
}

func (cli *SmtpClient) sendMail(toUser []string, msg []byte) error {
	return smtp.SendMail(cli.addr, cli.auth, cli.user, toUser, msg)
}

//SendEmail send email by string content
func (cli *SmtpClient) SendEmail(toUser []string, subject string, content string) error {
	msg := cli.generateEmailMsg(toUser, subject, content)
	if cli.isSSL {
		return cli.sendMailTLS(toUser, msg)
	}
	return cli.sendMail(toUser, msg)
}

//SendEmailByte send email by byte body
func (cli *SmtpClient) SendEmailByte(toUser []string, subject string, body []byte) error {
	msg := cli.generateEmailMsgByte(toUser, subject, body)
	if cli.isSSL {
		return cli.sendMailTLS(toUser, msg)
	}
	return cli.sendMail(toUser, msg)
}
