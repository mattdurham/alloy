package types

import (
	"github.com/vladopajic/go-actor/actor"
)

type Data struct {
	Meta map[string]string
	Data []byte
}

type DataHandle struct {
	Name string
	Get  func(name string) (map[string]string, []byte, error)
}

type FileStorage interface {
	Mailbox() actor.MailboxSender[Data]
	Stop()
}

type Serializer interface {
	Mailbox() actor.MailboxSender[[]*Raw]
	MetaMailbox() actor.MailboxSender[[]*RawMetadata]
}
