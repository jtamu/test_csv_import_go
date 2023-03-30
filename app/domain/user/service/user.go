package service

import (
	"my-s3-function-go/app/domain/user"
	"my-s3-function-go/app/infrastructure/repository"
	"my-s3-function-go/config"
)

type UserService struct {
	q          Queue
	repository *repository.UserRepository
}

func NewUserService(q Queue) *UserService {
	return &UserService{
		q:          q,
		repository: repository.NewUserRepository(),
	}
}

func (u *UserService) ImportUser(user *user.User) error {
	if err := u.validateEmail(user.Email); err != nil {
		return err
	}
	if err := u.q.SendMessage(user); err != nil {
		return err
	}
	return nil
}

func (u *UserService) validateEmail(email string) error {
	// TODO: 毎回クエリ発行をやめる
	emails, err := u.repository.GetAllEmails()
	if err != nil {
		return err
	}
	for _, otherEmail := range emails {
		if email == otherEmail {
			return config.NewValidationError("メールアドレスが重複しています")
		}
	}
	return nil
}

type Queue interface {
	SendMessage(any) error
}
