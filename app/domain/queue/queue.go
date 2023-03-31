package queue

type Queue interface {
	SendMessage(any) error
}
