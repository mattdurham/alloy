package types

import (
	"github.com/vladopajic/go-actor/actor"
)

type NetworkClient interface {
	Start()
	Stop()
	Mailbox() actor.MailboxSender[NetworkQueueItem]

	MetaMailbox() actor.MailboxSender[NetworkMetadataItem]
}
