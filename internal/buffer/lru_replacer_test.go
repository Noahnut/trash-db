package buffer

import (
	"go-db/internal/common/types"
	"testing"
)

func Test_LRU_Replacer_Simple(t *testing.T) {
	replacer := NewLRUReplacer()
	replacer.Unpin(1)
	if replacer.Size() != 1 {
		t.Error("queue wrong")
	}
	replacer.Pin(1)
	if replacer.Size() != 0 {
		t.Error("queue wrong")
	}
	replacer.Unpin(2)
	if replacer.Size() != 1 {
		t.Error("queue wrong")
	}

	frame_id := replacer.Victim()
	if frame_id != 2 {
		t.Error("victim wrong")
	}
}

func Test_LRU_Replacer_Multi(t *testing.T) {
	replacer := NewLRUReplacer()

	for i := types.Frame_id_t(0); i < 10; i++ {
		replacer.Unpin(i)
	}

	if replacer.Size() != 10 {
		t.Error("queue wrong")
	}

	replacer.Pin(2)
	replacer.Pin(4)
	replacer.Pin(6)

	if replacer.Size() != 7 {
		t.Error("queue wrong")
	}

	for i := types.Frame_id_t(0); i < 10; i++ {
		if i == 2 || i == 4 || i == 6 {
			continue
		}
		rframeID := replacer.Victim()
		if rframeID != i {
			t.Error("replacer wrong", rframeID)
		}
	}

	if replacer.Size() != 0 {
		t.Error("queue wrong")
	}
}
