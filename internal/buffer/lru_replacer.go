package buffer

import (
	"go-db/internal/common/constant"
	"go-db/internal/common/types"
	"sync"
)

type LRUReplacer struct {
	mutex        sync.Mutex
	replacerList []types.Frame_id_t
}

func NewLRUReplacer() *LRUReplacer {
	return &LRUReplacer{}
}

func (l *LRUReplacer) Victim() types.Frame_id_t {
	if len(l.replacerList) == 0 {
		return constant.INVALID_FRAME_ID
	}

	l.mutex.Lock()
	frameID := l.replacerList[0]
	l.remove(0)
	l.mutex.Unlock()
	return frameID
}

func (l *LRUReplacer) Pin(frameID types.Frame_id_t) {
	if len(l.replacerList) == 0 {
		return
	}

	for i, frame := range l.replacerList {
		if frame == frameID {
			l.mutex.Lock()
			l.remove(i)
			l.mutex.Unlock()
			break
		}
	}
}

func (l *LRUReplacer) Unpin(frameID types.Frame_id_t) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.replacerList = append(l.replacerList, frameID)
}

func (l *LRUReplacer) Size() int32 {
	return int32(len(l.replacerList))
}

func (l *LRUReplacer) remove(i int) {
	l.replacerList = append(l.replacerList[:i], l.replacerList[i+1:]...)
}
