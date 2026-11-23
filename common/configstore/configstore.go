/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configstore

import (
	"errors"
	"fmt"
	"sort"

	"github.com/Chaguar/orderer/common/filerepo"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"google.golang.org/protobuf/proto"
)

const (
	fileSuffix           = "config.pb.bin"
	fileNameSeparator    = "-"
	baseFileNameTemplate = "block" + fileNameSeparator + "%d"
)

// Store holds a set of numbered blocks, typically config blocks.
// Blocks can be added, listed, and retrieved.
// A convenience method allows fetching the last block.
type Store struct {
	fileRepo *filerepo.Repo
}

// NewStore initializes a new config store at the storeDir.
// All file system operations on the returned file repo are thread safe.
func NewStore(storeDir string) (*Store, error) {
	repo, err := filerepo.New(storeDir, fileSuffix)
	if err != nil {
		return nil, fmt.Errorf("failed to create config store: %w", err)
	}

	s := &Store{
		fileRepo: repo,
	}

	return s, nil
}

// Add adds a block to the store. One cannot add a block with the same block number twice.
func (s *Store) Add(block *common.Block) error {
	if block == nil {
		return errors.New("block is nil")
	}

	if block.Header == nil {
		return errors.New("block header is nil")
	}

	content, err := proto.Marshal(block)
	if err != nil {
		return fmt.Errorf("failed to marshal block: %w", err)
	}

	fileName := fmt.Sprintf(baseFileNameTemplate, block.Header.Number)

	err = s.fileRepo.Save(fileName, content)
	if err != nil {
		return fmt.Errorf("failed to add block %d: %w", block.GetHeader().GetNumber(), err)
	}

	return nil
}

// GetByNumber retrieves a block by its number.
func (s *Store) GetByNumber(num uint64) (*common.Block, error) {
	fileName := fmt.Sprintf(baseFileNameTemplate, num)
	return s.readByName(fileName)
}

// ListBlocks retrieves an array of all blocks, sorted by number.
func (s *Store) ListBlocks() ([]*common.Block, error) {
	fileNames, err := s.fileRepo.List()
	if err != nil {
		return nil, fmt.Errorf("failed to list blocks: %w", err)
	}

	var blocks []*common.Block
	for i, fileName := range fileNames {
		block, err := s.readByName(fileName)
		if err != nil {
			return nil, fmt.Errorf("failed reading block %s, %d out of %d; list: %v: %w", fileName, i+1, len(fileNames), fileNames, err)
		}

		blocks = append(blocks, block)
	}

	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].GetHeader().GetNumber() < blocks[j].GetHeader().GetNumber()
	})

	return blocks, nil
}

func (s *Store) readByName(fileName string) (*common.Block, error) {
	baseName := s.fileRepo.FileToBaseName(fileName)
	content, err := s.fileRepo.Read(baseName)
	if err != nil {
		return nil, fmt.Errorf("failed reading block %s: %w", fileName, err)
	}

	block := &common.Block{}
	err = proto.Unmarshal(content, block)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal block %s: %w", fileName, err)
	}

	return block, nil
}

// ListBlockNumbers retrieves a sorted array of all block numbers.
func (s *Store) ListBlockNumbers() ([]uint64, error) {
	fileNames, err := s.fileRepo.List()
	if err != nil {
		return nil, fmt.Errorf("failed to list blocks: %w", err)
	}

	var nums []uint64
	for i, fileName := range fileNames {
		block, err := s.readByName(fileName)
		if err != nil {
			return nil, fmt.Errorf("failed reading block %s, %d out of %d; list: %v: %w", fileName, i+1, len(fileNames), fileNames, err)
		}
		nums = append(nums, block.GetHeader().GetNumber())
	}

	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })

	return nums, nil
}

// Last retrieves the last (highest numbered) block.
func (s *Store) Last() (*common.Block, error) {
	nums, err := s.ListBlockNumbers()
	if err != nil {
		return nil, fmt.Errorf("failed to list block numbers: %w", err)
	}

	if len(nums) == 0 {
		return nil, errors.New("config store is empty")
	}

	return s.GetByNumber(nums[len(nums)-1])
}
