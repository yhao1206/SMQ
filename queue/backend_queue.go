package queue

type Queue interface {
	Get() ([]byte, error)
	Put([]byte) error
	ReadReadyChan() chan struct{}
	Close() error
}
