package repository

import "my-s3-function-go/app/domain/user"

type UserRepository struct {
	baseRepository *BaseRepository
}

func NewUserRepository() *UserRepository {
	return &UserRepository{baseRepository: baseRepository}
}

func (u *UserRepository) GetAllEmails() ([]string, error) {
	emails := []string{}
	if err := u.baseRepository.db.Model(&user.User{}).Pluck("email", &emails).Error; err != nil {
		return nil, err
	}
	return emails, nil
}
