/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"fmt"
	"os"

	"github.com/Chaguar/common/tools/fileutil"
)

type blockfileWriter struct {
	filePath string
	file     *os.File
}

func newBlockfileWriter(filePath string) (*blockfileWriter, error) {
	writer := &blockfileWriter{filePath: filePath}
	return writer, writer.open()
}

func (w *blockfileWriter) truncateFile(targetSize int) error {
	fileStat, err := w.file.Stat()
	if err != nil {
		return fmt.Errorf("error truncating the file [%s] to size [%d]: %w", w.filePath, targetSize, err)
	}
	if fileStat.Size() > int64(targetSize) {
		w.file.Truncate(int64(targetSize))
	}
	return nil
}

func (w *blockfileWriter) append(b []byte, sync bool) error {
	_, err := w.file.Write(b)
	if err != nil {
		return err
	}
	if sync {
		return w.file.Sync()
	}
	return nil
}

func (w *blockfileWriter) open() error {
	file, err := os.OpenFile(w.filePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o660)
	if err != nil {
		return fmt.Errorf("error opening block file writer for file %s: %w", w.filePath, err)
	}
	if err := fileutil.SyncParentDir(w.filePath); err != nil {
		return err
	}
	w.file = file
	return nil
}

func (w *blockfileWriter) close() error {
	return w.file.Close()
}
