package uuid

type IDGen interface {
	GetID(params ...interface{}) (Result, error)
	InitIDGen() (bool, error)
}
