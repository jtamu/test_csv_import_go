package service

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"my-s3-function-go/app/domain/importstatus"
	"reflect"
	"strings"

	"github.com/saintfish/chardet"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/transform"
)

type InvalidFileFormatError struct {
	msg string
}

func (i *InvalidFileFormatError) Error() string {
	return i.msg
}

func NewInvalidFileFormatError(msg string) *InvalidFileFormatError {
	return &InvalidFileFormatError{msg: msg}
}

func ConvertToUTF8(bytes []byte) ([]byte, error) {
	detector := chardet.NewTextDetector()
	result, err := detector.DetectBest(bytes)
	if err != nil {
		return nil, err
	}
	converted := []byte{}
	s := result.Charset
	switch {
	case s == "Shift_JIS" || s == "windows-1252":
		converted, err = io.ReadAll(transform.NewReader(strings.NewReader(string(bytes)), japanese.ShiftJIS.NewDecoder()))
		if err != nil {
			return nil, err
		}
	case s == "UTF-8" || strings.Contains(s, "ISO-8859"):
		converted = bytes
	default:
		return nil, NewInvalidFileFormatError("CSVファイルの文字コードが不正です")
	}
	return converted, nil
}

type InvalidHeaderError struct {
	notExistHeaders []string
}

func (i *InvalidHeaderError) Error() string {
	return fmt.Sprintf("CSVファイルのヘッダが欠損しています: %s", strings.Join(i.notExistHeaders, ","))
}

func NewInvalidHeaderError(notExistHeaders []string) *InvalidHeaderError {
	return &InvalidHeaderError{notExistHeaders: notExistHeaders}
}

func ValidateHeader[T any](csv []byte, importStatus *importstatus.ImportStatus) error {
	scanner := bufio.NewScanner(bytes.NewBuffer(csv))
	for scanner.Scan() {
		unquoted := strings.ReplaceAll(scanner.Text(), "\"", "")
		headers := strings.Split(unquoted, ",")

		notExistHeaders := []string{}

		model := new(T)
		t := reflect.TypeOf(*model)
	L:
		for i := 0; i < t.NumField(); i++ {
			csvTag := t.Field(i).Tag.Get("csv")
			for _, header := range headers {
				if header == csvTag {
					continue L
				}
			}
			notExistHeaders = append(notExistHeaders, csvTag)
		}
		if len(notExistHeaders) > 0 {
			return NewInvalidHeaderError(notExistHeaders)
		}
		// ヘッダのみでいいので1行読み終わったら抜ける
		break
	}
	return nil
}
