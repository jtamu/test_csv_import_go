package config

import (
	"reflect"
	"strings"

	ja_translations "gopkg.in/go-playground/validator.v9/translations/ja"

	"github.com/go-playground/locales/ja"
	ut "github.com/go-playground/universal-translator"
	"gopkg.in/go-playground/validator.v9"
)

var (
	Validate *Validator
)

func init() {
	Validate = NewValidate()
}

type Validator struct {
	validate *validator.Validate
	trans    ut.Translator
}

func NewValidate() *Validator {
	ja := ja.New()
	uni := ut.New(ja, ja)
	t, _ := uni.GetTranslator("ja")
	trans := t
	validate := validator.New()
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		fieldName := fld.Tag.Get("jaFieldName")
		if fieldName == "-" {
			return ""
		}
		return fieldName
	})
	ja_translations.RegisterDefaultTranslations(validate, trans)

	return &Validator{
		validate: validate,
		trans:    trans,
	}
}

func (v *Validator) Struct(obj any) error {
	if err := v.validate.Struct(obj); err != nil {
		return NewValidationError(strings.Join(v.getErrorMessages(err), ","))
	}
	return nil
}

func (v *Validator) getErrorMessages(err error) []string {
	if err == nil {
		return []string{}
	}
	var messages []string
	for _, m := range err.(validator.ValidationErrors).Translate(v.trans) {
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
