package buffer

import (
	"go-db/internal/common/constant"
	"go-db/internal/common/errors"
	"go-db/internal/common/types"
	"go-db/internal/storage/disk"
	"go-db/internal/storage/page"
	"log"
	"sync"
)

type BufferPoolManager struct {
	Replacer     IReplacer
	PoolSize     int32
	PageTable    map[types.Page_id_t]types.Frame_id_t
	FreePageList []types.Frame_id_t
	BufferPool   []*page.Page
	DiskManager  *disk.Disk
	Lock         sync.Mutex
}

func NewBufferPoolManager(replacer IReplacer, diskManager *disk.Disk, poolSize int32) *BufferPoolManager {
	bp := &BufferPoolManager{
		Replacer:    replacer,
		PoolSize:    poolSize,
		PageTable:   make(map[types.Page_id_t]types.Frame_id_t),
		BufferPool:  make([]*page.Page, poolSize),
		DiskManager: diskManager,
	}

	for i := 0; i < int(poolSize); i++ {
		bp.FreePageList = append(bp.FreePageList, types.Frame_id_t(i))
		bp.BufferPool[i] = page.NewPage()
	}

	return bp
}

func (p *BufferPoolManager) FetchPage(pageID types.Page_id_t) (*page.Page, error) {
	var frame_id types.Frame_id_t
	var err error
	frame_id, exist := p.PageTable[pageID]

	if !exist {
		frame_id = p.getFromFreeList()

		if frame_id == constant.INVALID_FRAME_ID {
			frame_id = p.replacePage()
			if frame_id == constant.INVALID_FRAME_ID {
				return nil, errors.ErrNoPageCanReplace
			}
		}
		page := p.BufferPool[frame_id]

		data, err := p.DiskManager.ReadPage(pageID)
		if err != nil {
			return nil, err
		}
		copy(page.GetData(), data)
	}

	page := p.BufferPool[frame_id]

	if err != nil {
		return nil, err
	}

	p.pinPage(page, frame_id)

	return page, nil
}

func (p *BufferPoolManager) NewPage() (*page.Page, error) {
	newPageID := p.DiskManager.AllocatePage()
	frame_id := p.getFromFreeList()

	if frame_id == constant.INVALID_FRAME_ID {
		frame_id = p.replacePage()
		if frame_id == constant.INVALID_FRAME_ID {
			return nil, errors.ErrNoPageCanReplace
		}
	}

	p.Lock.Lock()
	p.PageTable[newPageID] = frame_id
	p.Lock.Unlock()

	page := p.BufferPool[frame_id]
	page.ResetPageData()
	page.SetPageID(newPageID)
	p.pinPage(page, frame_id)

	return page, nil
}

func (p *BufferPoolManager) UnpinPage(pageID types.Page_id_t) bool {
	if frame_id, exist := p.PageTable[pageID]; exist {
		page := p.BufferPool[frame_id]
		p.unpinPage(page, frame_id)
	}

	return true
}

func (p *BufferPoolManager) FlushPage(pageID types.Page_id_t) {
	if frame_id, exist := p.PageTable[pageID]; exist {
		page := p.BufferPool[frame_id]
		err := p.flushPageData(page)
		if err != nil {
			log.Println(err)
		}
	}
}

func (p *BufferPoolManager) FlushAllPage() {
	for _, v := range p.PageTable {
		page := p.BufferPool[v]
		err := p.flushPageData(page)
		if err != nil {
			log.Println(err)
		}
	}
}

//TODO
func (p *BufferPoolManager) DeletePage(pageID types.Page_id_t) {

}

func (p *BufferPoolManager) getFromFreeList() types.Frame_id_t {
	frame_id := constant.INVALID_FRAME_ID
	if len(p.FreePageList) != 0 {
		frame_id = p.FreePageList[len(p.FreePageList)-1]
		p.FreePageList = p.FreePageList[:len(p.FreePageList)-1]
	}
	return frame_id
}

func (p *BufferPoolManager) flushPageData(page *page.Page) error {
	return p.DiskManager.WritePage(page.GetPageID(), page.GetData())
}

func (p *BufferPoolManager) replacePage() types.Frame_id_t {
	RFrame_id := p.Replacer.Victim()
	if RFrame_id == constant.INVALID_FRAME_ID {
		return constant.INVALID_FRAME_ID
	}

	oldPage := p.BufferPool[RFrame_id]
	if oldPage.IsDirty() {
		err := p.flushPageData(oldPage)
		if err != nil {
			log.Println(err)
			return constant.INVALID_FRAME_ID
		}
	}

	delete(p.PageTable, oldPage.GetPageID())
	oldPage.ResetPageData()

	return RFrame_id
}

func (p *BufferPoolManager) pinPage(page *page.Page, frame_id types.Frame_id_t) {
	page.AddPinCount()
	p.Replacer.Pin(frame_id)
}

func (p *BufferPoolManager) unpinPage(page *page.Page, frame_id types.Frame_id_t) {
	page.SubPinCount()
	if page.GetPinCount() == 0 {
		p.Replacer.Unpin(frame_id)
	}
}
