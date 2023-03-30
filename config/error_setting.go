package config

import (
	"reflect"

	ja_translations "gopkg.in/go-playground/validator.v9/translations/ja"

	"github.com/go-playground/locales/ja"
	ut "github.com/go-playground/universal-translator"
	"gopkg.in/go-playground/validator.v9"
)

var (
	uni   *ut.UniversalTranslator
	trans ut.Translator
)

func InitValidator(emails []string) *validator.Validate {
	ja := ja.New()
	uni = ut.New(ja, ja)
	t, _ := uni.GetTranslator("ja")
	trans = t
	validate := validator.New()
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		fieldName := fld.Tag.Get("jaFieldName")
		if fieldName == "-" {
			return ""
		}
		return fieldName
	})
	ja_translations.RegisterDefaultTranslations(validate, trans)

	return validate
}

func GetErrorMessages(err error) []string {
	if err == nil {
		return []string{}
	}
	var messages []string
	for _, m := range err.(validator.ValidationErrors).Translate(trans) {
		messages = append(messages, m)
	}
	return messages
}

type ValidationError struct {
	msg string
}

func (v *ValidationError) Error() string {
	return v.msg
}

func NewValidationError(msg string) *ValidationError {
	return &ValidationError{msg: msg}
}
