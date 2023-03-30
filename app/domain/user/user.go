package user

type User struct {
	ID    int    `csv:"id" jaFieldName:"ID" validate:"required"`
	Name  string `csv:"name" jaFieldName:"ユーザ名" validate:"required"`
	Email string `csv:"email" jaFieldName:"メールアドレス" validate:"required"`
}
