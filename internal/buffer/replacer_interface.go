package buffer

import (
	"go-db/internal/common/types"
)

type IReplacer interface {
	Victim() types.Frame_id_t
	Pin(frameID types.Frame_id_t)
	Unpin(frameID types.Frame_id_t)
	Size() int32
}
