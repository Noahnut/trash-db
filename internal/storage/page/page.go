package page

import (
	"encoding/binary"
	"go-db/internal/common/constant"
	"go-db/internal/common/types"
	"sync"
	"sync/atomic"
)

/*
+-----------------+
| Data(4096) .... |
+-----------------+
*/

const PAGE_TYPE_OFFSET = 4

type Page struct {
	mutex    sync.RWMutex
	pageID   types.Page_id_t
	pinCount int32
	isDirty  bool
	data     []byte
}

func NewPage() *Page {
	return &Page{
		data:     make([]byte, constant.PAGE_SIZE),
		isDirty:  false,
		pinCount: 0,
	}
}

func (p *Page) GetData() []byte {
	return p.data
}

func (p *Page) GetPageID() types.Page_id_t {
	return p.pageID
}

func (p *Page) GetPageTye() types.PAGE_TYPE {
	return types.PAGE_TYPE(binary.BigEndian.Uint32(p.data[:PAGE_TYPE_OFFSET]))
}

func (p *Page) SetPageType(pageType types.PAGE_TYPE) {
	binary.BigEndian.PutUint32(p.data[:PAGE_TYPE_OFFSET], uint32(pageType))
}

func (p *Page) ResetPageData() {
	p.data = make([]byte, constant.PAGE_SIZE)
}

func (p *Page) SetPageID(pageID types.Page_id_t) {
	p.pageID = pageID
}

func (p *Page) AddPinCount() {
	atomic.AddInt32(&p.pinCount, 1)
}

func (p *Page) SubPinCount() {
	atomic.AddInt32(&p.pinCount, -1)
}

func (p *Page) GetPinCount() int32 {
	return atomic.LoadInt32(&p.pinCount)
}

func (p *Page) IsDirty() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.isDirty
}

func (p *Page) SetDirty(isDirty bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.isDirty = isDirty
}
