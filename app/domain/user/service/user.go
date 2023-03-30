package service

import (
	"my-s3-function-go/app/domain/user"
)

type UserService struct {
	q Queue
}

func NewUserService(q Queue) *UserService {
	return &UserService{
		q: q,
	}
}

func (u *UserService) ImportUser(user *user.User) error {
	if err := u.q.SendMessage(user); err != nil {
		return err
	}
	return nil
}

type Queue interface {
	SendMessage(any) error
}
