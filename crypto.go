package oo

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"math/big"
)

type AesCrypt struct {
	Passwd   string `toml:"passwd,omitzero"`
	Ivstring string `toml:"ivstring,omitzero"`
}

// md5
func Md5(data []byte) []byte {
	md5Ctx := md5.New()
	md5Ctx.Write(data)
	return md5Ctx.Sum(nil)
}

// md5
func Md5Str(data []byte) string {
	return hex.EncodeToString(Md5(data))
}

// sha1
func Sha1(data []byte) []byte {
	h := sha1.New()
	return h.Sum(data)
}

// sha1
func Sha1Str(data []byte) string {
	return hex.EncodeToString(Sha1(data))
}

// hmac_sha1
func HmacSha1(data []byte, key []byte) []byte {
	h := hmac.New(sha1.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// hmac_sha1
func HmacSha1Str(data []byte, key []byte) string {
	return hex.EncodeToString(HmacSha1(data, key))
}

// hmac_sha256
func HmacSha256(data []byte, key []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// hmac_sha256
func HmacSha256Str(data []byte, key []byte) string {
	return hex.EncodeToString(HmacSha256(data, key))
}

// hash_time33
func HashTime33(str string) int {
	bStr := []byte(str)
	bLen := len(bStr)
	hash := 5381

	for i := 0; i < bLen; i++ {
		hash += ((hash << 5) & 0x7FFFFFF) + int(bStr[i])
	}
	return hash & 0x7FFFFFF
}

// base64
func Base64Encode(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

// base64
func Base64Decode(str string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(str)
}

var b58Alphabet = []byte("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

// Base58Encode encodes a byte array to Base58
func Base58Encode(input []byte) []byte {
	var result []byte

	x := big.NewInt(0).SetBytes(input)

	base := big.NewInt(int64(len(b58Alphabet)))
	zero := big.NewInt(0)
	mod := &big.Int{}

	for x.Cmp(zero) != 0 {
		x.DivMod(x, base, mod)
		result = append(result, b58Alphabet[mod.Int64()])
	}

	// https://en.bitcoin.it/wiki/Base58Check_encoding#Version_bytes
	if input[0] == 0x00 {
		result = append(result, b58Alphabet[0])
	}

	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return result
}

// Base58Decode decodes Base58-encoded data
func Base58Decode(input []byte) []byte {
	result := big.NewInt(0)

	for _, b := range input {
		charIndex := bytes.IndexByte(b58Alphabet, b)
		result.Mul(result, big.NewInt(58))
		result.Add(result, big.NewInt(int64(charIndex)))
	}

	decoded := result.Bytes()

	if len(input) > 0 && input[0] == b58Alphabet[0] {
		decoded = append([]byte{0x00}, decoded...)
	}

	return decoded
}

// aes
func AesEncrypt(passwd, ctx []byte) []byte {
	if len(passwd) > 0 {
		block, _ := aes.NewCipher(passwd)
		blockSize := block.BlockSize()
		padding := blockSize - len(ctx)%blockSize
		paddingText := bytes.Repeat([]byte{byte(padding)}, padding)
		ctx = append(ctx, paddingText...)
		blockMode := cipher.NewCBCEncrypter(block, passwd[:blockSize])
		plainText := make([]byte, len(ctx))
		blockMode.CryptBlocks(plainText, ctx)
		return plainText
	}
	return ctx
}

func AesDecrypt(passwd, ctx []byte) []byte {
	if len(passwd) > 0 {
		block, _ := aes.NewCipher(passwd)
		blockSize := block.BlockSize()
		blockMode := cipher.NewCBCDecrypter(block, passwd[:blockSize])
		plainText := make([]byte, len(ctx))
		blockMode.CryptBlocks(plainText, ctx)
		tLen := len(plainText)
		plainText = plainText[:tLen-int(plainText[tLen-1])]
		return plainText
	}
	return ctx
}
