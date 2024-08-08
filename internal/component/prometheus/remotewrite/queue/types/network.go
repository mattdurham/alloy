package types

import (
	"github.com/vladopajic/go-actor/actor"
)

type NetworkClient interface {
	Mailbox() actor.MailboxSender[NetworkQueueItem]

	MetaMailbox() actor.MailboxSender[NetworkMetadataItem]
}
