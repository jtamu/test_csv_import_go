package config

import (
	"reflect"

	ja_translations "gopkg.in/go-playground/validator.v9/translations/ja"

	"github.com/go-playground/locales/ja"
	ut "github.com/go-playground/universal-translator"
	"gopkg.in/go-playground/validator.v9"
)

var (
	uni      *ut.UniversalTranslator
	validate *validator.Validate
	trans    ut.Translator
)

func InitValidator(emails []string) {
	ja := ja.New()
	uni = ut.New(ja, ja)
	t, _ := uni.GetTranslator("ja")
	trans = t
	validate = validator.New()
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		fieldName := fld.Tag.Get("jaFieldName")
		if fieldName == "-" {
			return ""
		}
		return fieldName
	})
	ja_translations.RegisterDefaultTranslations(validate, trans)

	validate.RegisterValidation("email-unique", func(fl validator.FieldLevel) bool { return validateUniquenessOfEmail(fl, emails) })
	validate.RegisterTranslation("email-unique", trans, func(ut ut.Translator) error {
		trans.Add("email-unique", "{0}が重複しています", false)
		return nil
	}, func(ut ut.Translator, fe validator.FieldError) string {
		msg, _ := trans.T(fe.Tag(), fe.Field())
		return msg
	})
}

func validateUniquenessOfEmail(fl validator.FieldLevel, emails []string) bool {
	for _, email := range emails {
		if fl.Field().String() == email {
			return false
		}
	}
	return true
}

func ValidateStruct(s interface{}) error {
	return validate.Struct(s)
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
