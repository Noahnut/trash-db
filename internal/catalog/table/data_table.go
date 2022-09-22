package table

import (
	"encoding/binary"
	"go-db/internal/catalog/schema"
	"go-db/internal/catalog/tuple"
	"go-db/internal/common/constant"
	"go-db/internal/common/errors"
	"go-db/internal/common/types"
	"go-db/internal/storage/page"
	"sync"
)

/**
 *  DATA_TABLE_TYPE
 *  +--------+--------------------+-------------------------+
 *  | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
 *  +--------+--------------------+-------------------------+
 *                                ^
 *                                free space pointer
 *
 *  Header format (size in bytes):
 *  +-------------+---------------+---------------+---------------------+-------
 *  | PageType (4)| PrevPageId (4)| NextPageId (4)| FreeSpacePointer(4) |
 *  +-------------+---------------+---------------+---------------------+-------
 *  +----------------+--------------------+-------------------------
 *  | TupleCount (4) | Tuple_1 offset (4) | Tuple_1 size (4) | ... |
 *  +----------------+--------------------+-------------------------
 *
 *
 *  TUPLE format !! Carefully tuple should match the
 *  +-----------------+----------------+-------------+
 *  | Data 1 Payload  | Data 2 Payload | ...
 *  +-----------------+----------------+-------------+
 *
 */

type DataTable struct {
	*page.Page
	RLock sync.RWMutex
}

func GetDataTable(page *page.Page) *DataTable {
	return &DataTable{Page: page}
}

func (p *DataTable) DataTableInit() {
	p.SetFreeSpacePointer(constant.PAGE_SIZE)
	p.SetNextPageID(constant.INVALID_PAGE_ID)
	p.SetPageType(types.DATA_PAGE_TYPE)
}

func (p *DataTable) GetPrevPageID() types.Page_id_t {
	return types.Page_id_t(binary.BigEndian.Uint32(p.GetData()[types.PAGE_TYPE_OFFSET:types.PREV_PAGE_ID_OFFSET]))
}

func (p *DataTable) SetPrevPageID(pageID types.Page_id_t) {
	binary.BigEndian.PutUint32(p.GetData()[types.PAGE_TYPE_OFFSET:types.PREV_PAGE_ID_OFFSET], uint32(pageID))
}

func (p *DataTable) GetNextPageID() types.Page_id_t {
	return types.Page_id_t(binary.BigEndian.Uint32(p.GetData()[types.PREV_PAGE_ID_OFFSET:types.NEXT_PAGE_ID_OFFSET]))
}

func (p *DataTable) SetNextPageID(pageID types.Page_id_t) {
	binary.BigEndian.PutUint32(p.GetData()[types.PREV_PAGE_ID_OFFSET:types.NEXT_PAGE_ID_OFFSET], uint32(pageID))
}

func (p *DataTable) GetFreeSpacePointer() int32 {
	return int32(binary.BigEndian.Uint32(p.GetData()[types.NEXT_PAGE_ID_OFFSET:types.FREE_SPACE_POINTER_OFFSET]))
}

func (p *DataTable) SetFreeSpacePointer(pointer int32) {
	binary.BigEndian.PutUint32(p.GetData()[types.NEXT_PAGE_ID_OFFSET:types.FREE_SPACE_POINTER_OFFSET], uint32(pointer))
}

func (p *DataTable) GetTupleCount() int32 {
	return int32(binary.BigEndian.Uint32(p.GetData()[types.FREE_SPACE_POINTER_OFFSET:types.TUPLE_COUNT_OFFSET]))
}

func (p *DataTable) SetTupleCount(tupleCount int32) {
	binary.BigEndian.PutUint32(p.GetData()[types.FREE_SPACE_POINTER_OFFSET:types.TUPLE_COUNT_OFFSET], uint32(tupleCount))
}

func (p *DataTable) getTupleMetaByIndex(index int32) (int32, int32, error) {
	if index > p.GetTupleCount() || index < 0 {
		return -1, -1, errors.ErrIndexOutOfRange
	}

	metaOffset := types.TUPLE_COUNT_OFFSET + ((index) * (types.TUPLE_OFFSET + types.TUPLE_SIZE))

	var (
		tupleOffset int32
		tupleSize   int32
	)

	tupleSizeEnd, tupleOffsetEnd := metaOffset+types.TUPLE_SIZE, metaOffset+types.TUPLE_SIZE+types.TUPLE_OFFSET

	tupleOffset = int32(binary.BigEndian.Uint32(p.GetData()[metaOffset:tupleSizeEnd]))
	tupleSize = int32(binary.BigEndian.Uint32(p.GetData()[tupleSizeEnd:tupleOffsetEnd]))

	return tupleOffset, tupleSize, nil
}

func (p *DataTable) GetRemainSpace() int32 {
	tupleMetaOffset := types.TUPLE_COUNT_OFFSET + (p.GetTupleCount() * (types.TUPLE_OFFSET + types.TUPLE_SIZE))
	return p.GetFreeSpacePointer() - tupleMetaOffset
}

func (p *DataTable) InsertTuple(value []*tuple.Value, tupleSize int32) error {
	tupleCount := p.GetTupleCount()

	tupleData := tuple.TupleSerialization(value)

	tupleOffset := types.TUPLE_COUNT_OFFSET + ((types.TUPLE_OFFSET + types.TUPLE_SIZE) * tupleCount)

	freeSpacePoint := p.GetFreeSpacePointer()

	copy(p.GetData()[freeSpacePoint-tupleSize:freeSpacePoint], tupleData)

	p.SetFreeSpacePointer(p.GetFreeSpacePointer() - tupleSize)
	tupleSizeEnd, tupleOffsetEnd := tupleOffset+types.TUPLE_SIZE, tupleOffset+types.TUPLE_SIZE+types.TUPLE_OFFSET
	binary.BigEndian.PutUint32(p.GetData()[tupleOffset:tupleSizeEnd], uint32(p.GetFreeSpacePointer()))
	binary.BigEndian.PutUint32(p.GetData()[tupleSizeEnd:tupleOffsetEnd], uint32(tupleSize))

	p.SetTupleCount(tupleCount + 1)

	return nil
}

func (p *DataTable) GetTuple(schema *schema.Schema) [][]*tuple.Value {
	tupleCount := p.GetTupleCount()

	tuples := make([][]*tuple.Value, 0, tupleCount)

	for i := int32(0); i < tupleCount; i++ {
		tuple := p.getTupleByIndex(i, schema)

		if tuple != nil {
			tuples = append(tuples, tuple)
		}
	}

	return tuples
}

func (p *DataTable) getTupleByIndex(tupleIndex int32, schema *schema.Schema) []*tuple.Value {
	offset, size, _ := p.getTupleMetaByIndex(tupleIndex)

	tuplesData := p.GetData()[offset : offset+size]

	return tuple.TupleDeserialization(schema, tuplesData)
}
