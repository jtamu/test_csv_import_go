package service

import (
	"my-s3-function-go/app/domain/queue"
	"my-s3-function-go/app/domain/user"
	"my-s3-function-go/app/infrastructure/repository"
	"my-s3-function-go/config"
)

type UserService struct {
	q                queue.Queue
	repository       *repository.UserRepository
	registeredEmails []string
}

func NewUserService(q queue.Queue) *UserService {
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
	if u.registeredEmails == nil {
		emails, err := u.repository.GetAllEmails()
		if err != nil {
			return err
		}
		u.registeredEmails = emails
	}
	for _, otherEmail := range u.registeredEmails {
		if email == otherEmail {
			return config.NewValidationError("メールアドレスが重複しています")
		}
	}
	return nil
}
