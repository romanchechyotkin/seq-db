package frac

import (
	"fmt"
	"math"
	"path"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/ozontech/seq-db/buildinfo"
	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/seq"
)

const DistributionMaxInterval = 24 * time.Hour
const DistributionBucket = time.Minute
const DistributionSpreadThreshold = 10 * time.Minute

type Info struct {
	Path          string                   `json:"name"`
	Ver           string                   `json:"ver"`
	BinaryDataVer config.BinaryDataVersion `json:"binary_data_ver"`
	DocsTotal     uint32                   `json:"docs_total"`
	DocsOnDisk    uint64                   `json:"docs_on_disk"`  // how much compressed docs data is stored on disk
	DocsRaw       uint64                   `json:"docs_raw"`      // how much raw docs data is appended
	MetaOnDisk    uint64                   `json:"meta_on_disk"`  // how much compressed metadata is stored on disk
	IndexOnDisk   uint64                   `json:"index_on_disk"` // how much compressed index data is stored on disk

	ConstRegularBlockSize int `json:"const_regular_block_size"`
	ConstIDsPerBlock      int `json:"const_ids_per_block"`
	ConstLIDBlockCap      int `json:"const_lid_block_cap"`

	From         seq.MID               `json:"from"`
	To           seq.MID               `json:"to"`
	CreationTime uint64                `json:"creation_time"`
	SealingTime  uint64                `json:"sealing_time"`
	Distribution *seq.MIDsDistribution `json:"distribution"`
}

func NewInfo(filename string, docsOnDisk, metaOnDisk uint64) *Info {
	return &Info{
		Ver:                   buildinfo.Version,
		BinaryDataVer:         config.BinaryDataV1,
		Path:                  filename,
		From:                  math.MaxUint64,
		To:                    0,
		CreationTime:          uint64(time.Now().UnixMilli()),
		ConstIDsPerBlock:      consts.IDsPerBlock,
		ConstRegularBlockSize: consts.RegularBlockSize,
		ConstLIDBlockCap:      consts.LIDBlockCap,
		DocsOnDisk:            docsOnDisk,
		MetaOnDisk:            metaOnDisk,
	}
}

func (s *Info) String() string {
	return fmt.Sprintf(
		"raw docs=%s, disk docs=%s",
		datasize.ByteSize(s.DocsRaw).HR(),
		datasize.ByteSize(s.DocsOnDisk).HR(),
	)
}

func (s *Info) Name() string {
	if s.Path == "" {
		return ""
	}
	return path.Base(s.Path)
}

func (s *Info) BuildDistribution(ids []seq.ID) {
	if !s.InitEmptyDistribution() {
		return
	}
	for _, id := range ids {
		s.Distribution.Add(id.MID)
	}
}

func (s *Info) InitEmptyDistribution() bool {
	from := time.UnixMilli(int64(s.From))
	creationTime := time.UnixMilli(int64(s.CreationTime))
	if creationTime.Sub(from) < DistributionSpreadThreshold { // no big spread in past
		return false
	}

	distTo := creationTime
	distFrom := from

	if distTo.Sub(distFrom) > DistributionMaxInterval {
		distFrom = distTo.Add(-DistributionMaxInterval)
	}

	s.Distribution = seq.NewMIDsDistribution(distFrom, distTo, DistributionBucket)
	return true
}

func (s *Info) FullSize() uint64 {
	return s.DocsOnDisk + s.IndexOnDisk + s.MetaOnDisk
}

func (s *Info) IsIntersecting(from, to seq.MID) bool {
	if s.DocsTotal == 0 { // don't include fresh active fraction
		return false
	}

	if to < s.From || s.To < from {
		return false
	}

	if s.Distribution == nil { // can't check distribution
		return true
	}

	// check with distribution
	return s.Distribution.IsIntersecting(from, to)
}
